#ifndef RYAN_DS_KV_RAFT_RAFT_H_
#define RYAN_DS_KV_RAFT_RAFT_H_

#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "buttonrpc/buttonrpc.hpp"
#include "kv_raft/semaphore.hpp"

constexpr int COMMOM_PORT = 1234;
constexpr int HEART_BEART_PERIOD = 100000;

 /// @brief
 /// 和LAB2相比修改挺大的, 增加了同kvServer应用层交互的代码，以及处理应用层快照的逻辑
 /// 新增了installSnapShotRPC，即在心跳中除了append分支还多了安装快照的分支，
 /// 由于快照会截断日志，所以原先和日志长度、索引等有关的逻辑全得重新改
 /// 同时由于C++和go的差异，许多协程能实现的地方需要多很多同步信息，
 /// 需要重新设置关于appendLoop中append和install的RPC端口信息以及对应客户端关系
 /// 直接看raft的类定义，里面对新增的函数及成员做了简单注释

//新增的快照RPC需要传的参数，具体看论文section7关于日志压缩的内容
struct InstallSnapShotArgs {
  int term{};
  int leader_id{};
  int last_included_index{};
  int last_included_term{};
  std::string snapshot{};

  friend Serializer& operator>>(Serializer& in, InstallSnapShotArgs& d) {
    in >> d.term >> d.leader_id >> d.last_included_index >>
        d.last_included_term >> d.snapshot;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, const InstallSnapShotArgs& d) {
    out << d.term << d.leader_id << d.last_included_index
        << d.last_included_term << d.snapshot;
    return out;
  }
};

struct InstallSnapSHotReply {
  int term;
};

struct Operation {
  std::string op{};
  std::string key{};
  std::string value{};
  int client_id{};
  int request_id{};
  int term{};
  int index{};

  std::string GetCmd() const {
    return {op + " " + key + " " + value + " " +
                      std::to_string(client_id) + " " +
                      std::to_string(request_id)};
  }
};



struct StartRet {
  int cmd_index{-1};
  int curr_term{-1};
  bool is_leader{false};
};

struct ApplyMsg {
  bool is_command_valid{};
  std::string command;
  int command_index{};
  int command_term{};
  int last_included_index{};
  int last_included_term{};
  std::string snapshot;

  Operation GetOperation();
};

Operation ApplyMsg::GetOperation() {
  Operation operation;
  std::vector<std::string> str;
  std::string tmp;
  for (char c : this->command) {
    if (c != ' ') {
      tmp += c;
    } else {
      if (!tmp.empty()) str.push_back(tmp);
      tmp = "";
    }
  }
  if (!tmp.empty()) {
    str.push_back(tmp);
  }
  operation.op = str[0];
  operation.key = str[1];
  operation.value = str[2];
  operation.client_id = std::stoi(str[3]);
  operation.request_id = std::stoi(str[4]);
  operation.term = command_term;
  return operation;
}

struct PeersInfo {
  std::pair<int, int> port;
  int peer_id;
  bool is_install_flag;
};

struct LogEntry {
  explicit LogEntry(std::string cmd = "", int term = -1)
      : command(std::move(cmd)), term(term) {}
  std::string command;
  int term;
};

struct Persister {
  std::vector<LogEntry> logs;
  std::string snapshot;
  int curr_term;
  int voted_for;
  int last_included_index;
  int last_included_term;
};

struct AppendEntriesArgs {
 public:
  int term{};
  int leader_id{};
  int prev_log_index{};
  int prev_log_term{};
  int leader_commit{};
  std::string entrys;
  friend Serializer& operator>>(Serializer& in, AppendEntriesArgs& d) {
    in >> d.term >> d.leader_id >> d.prev_log_index >> d.prev_log_term >>
        d.leader_commit >> d.entrys;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, const AppendEntriesArgs& d) {
    out << d.term << d.leader_id << d.prev_log_index << d.prev_log_term
        << d.leader_commit << d.entrys;
    return out;
  }
};

struct AppendEntriesReply {
  int term;
  bool success;
  int conflict_term;
  int conflict_index;
};

struct RequestVoteArgs {
  int term;
  int candidate_id;
  int last_log_term;
  int last_log_index;
};

struct RequestVoteReply {
  int term;
  bool vote_granted;
};

class Raft {
 public:
  void ListenForVote();
  void ListenForAppend();
  void ProcessEntriesLoop();
  void ElectionLoop();
  void CallRequestVote();
  void SendAppendEntries();  //向其他follower发送快照的函数，处理逻辑看论文
  void SendInstallSnapShot();
  void ApplyLogLoop();

  enum RAFT_STATE { LEADER = 0, CANDIDATE, FOLLOWER };
  void Make(const std::vector<PeersInfo>& peers, int id);
  static int GetMyduration(timeval last);
  void SetBroadcastTime();
  std::pair<int, bool> GetState();
  RequestVoteReply RequestVote(RequestVoteArgs args);
  AppendEntriesReply AppendEntries(const AppendEntriesArgs& args);

  //安装快照的RPChandler，处理逻辑看论文
  InstallSnapSHotReply InstallSnapShot(const InstallSnapShotArgs& args);  

  bool CheckLogUptodate(int term, int index);
  void PushBackLog(const LogEntry& log);
  static std::vector<LogEntry> GetCmdAndTerm(std::string text);
  StartRet Start(const Operation& op);
  void PrintLogs();

  //初始化send的信号量，结合kvServer层的有名管道fifo模拟go的select及channel
  void SetSendSem(int num);  
  //初始化recv的信号量，结合kvServer层的有名管道fifo模拟go的select及channel
  void SetRecvSem(int num);  

  bool WaitSendSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  bool WaitRecvSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  bool PostSendSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  bool PostRecvSem();  //信号量函数封装，用于类复合时kvServer的类外调用

  //取得一个msg，结合信号量和fifo模拟go的select及channel，每次只取一个，处理完再取
  ApplyMsg GetBackMsg();  

  void Serialize();
  bool Deserialize();
  void SaveRaftState();
  void ReadRaftState();
  bool IsKilled();  //->check is killed?
  void Kill();
  void Activate();

  //超出日志大小则需要快照，kvServer层需要有个守护线程持续调用该函数判断
  bool ExceedLogSize(int size);  

  //接受来自kvServer层的快照，用于持久化
  void RecvSnapShot(std::string snap_shot, int last_included_index);  

  int IdxToCompressLogPos(int index) const;  //获得原先索引在截断日志后的索引
  bool ReadSnapShot();                 //读取快照
  void SaveSnapShot() const;                 //持久化快照
  void InstallSnapShotTokvServer();  //落后的raft向对应的应用层安装快照
  int LastIndex();                   //截断日志后的lastIndex
  int LastTerm();                    //截断日志后的lastTerm

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  std::vector<PeersInfo> peers_;
  Persister persister_;
  int peer_id_;
  bool dead_;

  //需要持久化的data
  int curr_term_;
  int voted_for_;
  std::vector<LogEntry> logs_;
  int last_included_index_;  //新增的持久化变量，存上次快照日志截断处的相关信息
  int last_included_term_;  //新增的持久化变量，存上次快照日志截断处的相关信息

  std::vector<int> next_index_;
  std::vector<int> match_index_;
  int last_applied_;
  int commit_index_;

  int recv_votes_;
  int finished_vote_;
  int curr_peer_id_;

  RAFT_STATE state_;
  int leader_id_;
  struct timeval last_wake_time_;
  struct timeval last_broadcast_time_;

  //用作与kvRaft交互
  Semaphore recv_sem_;  //结合kvServer层的有名管道fifo模拟go的select及channel
  Semaphore send_sem_;  //结合kvServer层的有名管道fifo模拟go的select及channel
  std::vector<ApplyMsg>
      msgs_;  //在applyLogLoop中存msg的容易，每次存一条，处理完再存一条

  // bool installSnapShotFlag;
  // bool applyLogFlag;
  std::unordered_set<int>
      is_exist_index_;  //用于在processEntriesLoop中标识append和install端口对应分配情况
};

void Raft::Make(const std::vector<PeersInfo>& peers, int id) {
  peers_ = peers;
  // this->persister_ = persister_;
  peer_id_ = id;
  dead_ = false;

  state_ = FOLLOWER;
  curr_term_ = 0;
  leader_id_ = -1;
  voted_for_ = -1;
  ::gettimeofday(&last_wake_time_, nullptr);
  // readPersist(persister_.ReadRaftState());

  // for(int i = 0; i < id + 1; i++){
  //     LogEntry log;
  //     log.command_str = std::to_string(i);
  //     log.term = i;
  //     logs_.push_back(log);
  // }

  recv_votes_ = 0;
  finished_vote_ = 0;
  curr_peer_id_ = 0;

  last_applied_ = 0;
  commit_index_ = 0;
  next_index_.resize(peers.size(), 1);
  match_index_.resize(peers.size(), 0);

  last_included_index_ = 0;
  last_included_term_ = 0;
  is_exist_index_.clear();

  ReadRaftState();
  InstallSnapShotTokvServer();
 
  std::thread(&Raft::ListenForVote, this).detach();
  std::thread(&Raft::ListenForAppend, this).detach();
  std::thread(&Raft::ApplyLogLoop, this).detach();
}

void Raft::ApplyLogLoop() {
  
  while (true) {
    while (!dead_) {
      // std::unique_lock<std::mutex> lock(mutex_);
      // if(installSnapShotFlag){
      //     printf("%d check install : %d, apply : %d\n", peer_id_,
      //         installSnapShotFlag? 1 : 0, applyLogFlag ? 1 : 0);
      //     applyLogFlag = false;
      //     lock.unlock();
      //     ::usleep(10000);
      //     continue;
      // }
      // lock.unlock();
      ::usleep(10000);
      // printf("%d's apply is called, apply is %d, commit is %d\n",
      // peer_id_, last_applied_, commit_index_);
      std::vector<ApplyMsg> msgs;
      std::unique_lock<std::mutex> lock(mutex_);
      while (last_applied_ < commit_index_) {
        last_applied_++;
        int appliedIdx = IdxToCompressLogPos(last_applied_);
        ApplyMsg msg;
        msg.command = logs_[appliedIdx].command;
        msg.is_command_valid = true;
        msg.command_term = logs_[appliedIdx].term;
        msg.command_index = last_applied_;
        msgs.push_back(msg);
      }
      lock.unlock();
      for (auto & msg : msgs) {
        // printf("before %d's apply is called, apply is %d, commit is %d\n",
        //     peer_id_, last_applied_, commit_index_);
        WaitRecvSem();
        // printf("after %d's apply is called, apply is %d, commit is %d\n",
        //     peer_id_, last_applied_, commit_index_);
        msgs_.push_back(msg);
        PostSendSem();
      }
    }
    ::usleep(10000);
  }
}

int Raft::GetMyduration(timeval last) {
  struct timeval now{};
  ::gettimeofday(&now, nullptr);
  // printf("--------------------------------\n");
  // printf("now's sec : %ld, now's usec : %ld\n", now.tv_sec, now.tv_usec);
  // printf("last's sec : %ld, last's usec : %ld\n", last.tv_sec, last.tv_usec);
  // printf("%d\n", ((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec -
  // last.tv_usec))); printf("--------------------------------\n");
  return static_cast<int>((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec - last.tv_usec));
}

void Raft::SetBroadcastTime() {
  ::gettimeofday(&last_broadcast_time_, nullptr);
  printf("before : %ld, %ld\n", last_broadcast_time_.tv_sec,
         last_broadcast_time_.tv_usec);
  if (last_broadcast_time_.tv_usec >= 200000) {
    last_broadcast_time_.tv_usec -= 200000;
  } else {
    last_broadcast_time_.tv_sec -= 1;
    last_broadcast_time_.tv_usec += (1000000 - 200000);
  }
}

void Raft::ListenForVote() {
  
  buttonrpc server;
  server.as_server(peers_[peer_id_].port.first);
  server.bind("RequestVote", &Raft::RequestVote, this);

  std::thread(&Raft::ElectionLoop, this).detach();

  server.run();
  printf("std::exit!\n");
}

void Raft::ListenForAppend() {
  
  buttonrpc server;
  server.as_server(peers_[peer_id_].port.second);
  server.bind("AppendEntries", &Raft::AppendEntries, this);
  server.bind("InstallSnapShot", &Raft::InstallSnapShot, this);

  std::thread(&Raft::ProcessEntriesLoop, this).detach();

  server.run();
  printf("std::exit!\n");
}

void Raft::ElectionLoop() {
  bool resetFlag = false;
  while (!dead_) {
    int timeOut = std::rand() % 200000 + 200000;
    while (true) {
      ::usleep(1000);
      std::unique_lock<std::mutex> lock(mutex_);

      int during_time = GetMyduration(last_wake_time_);
      if (state_ == FOLLOWER && during_time > timeOut) {
        state_ = CANDIDATE;
      }

      if (state_ == CANDIDATE && during_time > timeOut) {
        printf(" %d attempt election at term %d, timeOut is %d\n",
               peer_id_, curr_term_, timeOut);
        ::gettimeofday(&last_wake_time_, nullptr);
        resetFlag = true;
        curr_term_++;
        voted_for_ = peer_id_;
        SaveRaftState();

        recv_votes_ = 1;
        finished_vote_ = 1;
        curr_peer_id_ = 0;

        for (auto server : peers_) {
          if (server.peer_id == peer_id_) continue;
          std::thread(&Raft::CallRequestVote, this).detach();
        }

        while (recv_votes_ <= peers_.size() / 2 &&
               finished_vote_ != peers_.size()) {
          cond_.wait(lock);
        }
        if (state_ != CANDIDATE) {
          continue;
        }
        if (recv_votes_ > peers_.size() / 2) {
          state_ = LEADER;

          for (int i = 0; i < peers_.size(); i++) {
            next_index_[i] = LastIndex() + 1;
            match_index_[i] = 0;
          }

          printf(" %d become new leader at term %d\n", peer_id_,
                 curr_term_);
          SetBroadcastTime();
        }
      }
      lock.unlock();
      if (resetFlag) {
        resetFlag = false;
        break;
      }
    }
  }
}

void Raft::CallRequestVote() {
  buttonrpc client;
  std::unique_lock<std::mutex> lock(mutex_);
  RequestVoteArgs args{};
  args.candidate_id = peer_id_;
  args.term = curr_term_;
  args.last_log_index = LastIndex();
  args.last_log_term = LastTerm();
  if (curr_peer_id_ == peer_id_) {
    curr_peer_id_++;
  }
  int clientPeerId = curr_peer_id_;
  client.as_client("127.0.0.1", peers_[curr_peer_id_++].port.first);

  if (curr_peer_id_ == peers_.size() ||
      (curr_peer_id_ == peers_.size() - 1 &&
       peer_id_ == curr_peer_id_)) {
    curr_peer_id_ = 0;
  }
  lock.unlock();

  RequestVoteReply reply =
      client.call<RequestVoteReply>("RequestVote", args).val();
  lock.lock();
  finished_vote_++;
  cond_.notify_one();
  if (reply.term > curr_term_) {
    state_ = FOLLOWER;
    curr_term_ = reply.term;
    voted_for_ = -1;
    ReadRaftState();
    return;
  }
  if (reply.vote_granted) {
    recv_votes_++;
  }
}

bool Raft::CheckLogUptodate(int term, int index) {
  int last_term = this->LastTerm();
  if (term > last_term) {
    return true;
  }
  if (term == last_term && index >= LastIndex()) {
    return true;
  }
  return false;
}

RequestVoteReply Raft::RequestVote(RequestVoteArgs args) {
  RequestVoteReply reply{};
  reply.vote_granted = false;
  std::lock_guard<std::mutex> lock(mutex_);
  reply.term = curr_term_;

  if (curr_term_ > args.term) {
    return reply;
  }

  if (curr_term_ < args.term) {
    state_ = FOLLOWER;
    curr_term_ = args.term;
    voted_for_ = -1;
  }

  if (voted_for_ == -1 || voted_for_ == args.candidate_id) {
    bool ret = CheckLogUptodate(args.last_log_term, args.last_log_index);
    if (!ret) {
      return reply;
    }
    voted_for_ = args.candidate_id;
    reply.vote_granted = true;
    printf("[%d] vote to [%d] at %d, duration is %d\n", peer_id_,
           args.candidate_id, curr_term_, GetMyduration(last_wake_time_));
    ::gettimeofday(&last_wake_time_, nullptr);
  }
  SaveRaftState();
  return reply;
}

void Raft::ProcessEntriesLoop() {
  while (!dead_) {
    ::usleep(1000);
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ != LEADER) {
      continue;
    }
    // printf("sec : %ld, usec : %ld\n", last_broadcast_time_.tv_sec,
    // last_broadcast_time_.tv_usec);
    int during_time = GetMyduration(last_broadcast_time_);
    // printf("time is %d\n", during_time);
    if (during_time < HEART_BEART_PERIOD) {
      continue;
    }

    ::gettimeofday(&last_broadcast_time_, nullptr);
    // printf("%d send AppendRetries at %d\n", peer_id_,
    // curr_term_); lock.unlock();

    for (auto& server : peers_) {
      if (server.peer_id == peer_id_) continue;
      if (next_index_[server.peer_id] <=
          last_included_index_) {  //进入install分支的条件，日志落后于leader的快照
        printf(
            "%d send install rpc to %d, whose nextIdx is %d, but leader's "
            "lastincludeIdx is %d\n",
            peer_id_, server.peer_id, next_index_[server.peer_id],
            last_included_index_);
        server.is_install_flag = true;
        std::thread(&Raft::SendInstallSnapShot, this).detach();
      } else {
        printf(
            "%d send append rpc to %d, whose nextIdx is %d, but leader's "
            "lastincludeIdx is %d\n",
            peer_id_, server.peer_id, next_index_[server.peer_id],
            last_included_index_);
        std::thread(&Raft::SendAppendEntries, this).detach();
      }
    }
  }
}

void Raft::SendInstallSnapShot() {
  
  buttonrpc client;
  InstallSnapShotArgs args;
  int clientPeerId;
  std::unique_lock<std::mutex> lock(mutex_);
  // for(int i = 0; i < peers_.size(); i++){
  //     printf("in install %d's server.is_install_flag is %d\n", i,
  //     peers_[i].is_install_flag ? 1 : 0);
  // }
  for (int i = 0; i < peers_.size(); i++) {
    if (peers_[i].peer_id == peer_id_) {
      // printf("%d is leader, continue\n", i);
      continue;
    }
    if (!peers_[i].is_install_flag) {
      // printf("%d is append, continue\n", i);
      continue;
    }
    if (is_exist_index_.count(i)) {
      // printf("%d is chongfu, continue\n", i);
      continue;
    }
    clientPeerId = i;
    is_exist_index_.insert(i);
    // printf("%d in install insert index : %d, size is %d\n", peer_id_,
    // i, is_exist_index_.size());
    break;
  }

  client.as_client("127.0.0.1", peers_[clientPeerId].port.second);

  if (is_exist_index_.size() == peers_.size() - 1) {
    // printf("install clear size is %d\n", is_exist_index_.size());
    for (auto & peer : peers_) {
      peer.is_install_flag = false;
    }
    is_exist_index_.clear();
  }

  args.last_included_index = last_included_index_;
  args.last_included_term = last_included_term_;
  args.leader_id = peer_id_;
  args.term = curr_term_;
  ReadSnapShot();
  args.snapshot = persister_.snapshot;

  printf("in send install snap_shot is %s\n", args.snapshot.c_str());

  lock.unlock();
  // printf("%d send to %d's install port is %d\n", peer_id_,
  // clientPeerId, peers_[clientPeerId].port.second);
  InstallSnapSHotReply reply =
      client.call<InstallSnapSHotReply>("InstallSnapShot", args).val();
  // printf("%d is called send install to %d\n", peer_id_, clientPeerId);

  lock.lock();
  if (curr_term_ != args.term) {
    return;
  }

  if (curr_term_ < reply.term) {
    state_ = FOLLOWER;
    voted_for_ = -1;
    curr_term_ = reply.term;
    SaveRaftState();
    return;
  }

  next_index_[clientPeerId] = LastIndex() + 1;
  match_index_[clientPeerId] = args.last_included_index;

  match_index_[peer_id_] = LastIndex();
  std::vector<int> tmpIndex = match_index_;
  std::sort(tmpIndex.begin(), tmpIndex.end());
  int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
  if (realMajorityMatchIndex > commit_index_ &&
      (realMajorityMatchIndex <= last_included_index_ ||
       logs_[IdxToCompressLogPos(realMajorityMatchIndex)].term ==
           curr_term_)) {
    commit_index_ = realMajorityMatchIndex;
  }
}

InstallSnapSHotReply Raft::InstallSnapShot(const InstallSnapShotArgs& args) {
  InstallSnapSHotReply reply{};
  std::unique_lock<std::mutex> lock(mutex_);
  reply.term = curr_term_;

  if (args.term < curr_term_) {
    return reply;
  }

  if (args.term >= curr_term_) {
    if (args.term > curr_term_) {
      voted_for_ = -1;
      SaveRaftState();
    }
    curr_term_ = args.term;
    state_ = FOLLOWER;
  }
  ::gettimeofday(&last_wake_time_, nullptr);

  printf("in stall rpc, args.last is %d, but selfLast is %d, size is %d\n",
         args.last_included_index, last_included_index_, LastIndex());
  if (args.last_included_index <= last_included_index_) {
    return reply;
  } else {
    if (args.last_included_index < LastIndex()) {
      if (logs_[IdxToCompressLogPos(LastIndex())].term !=
          args.last_included_term) {
        logs_.clear();
      } else {
        std::vector<LogEntry> tmpLog(
            logs_.begin() + IdxToCompressLogPos(args.last_included_index) + 1,
            logs_.end());
        logs_ = tmpLog;
      }
    } else {
      logs_.clear();
    }
  }

  last_included_index_ = args.last_included_index;
  last_included_term_ = args.last_included_term;
  persister_.snapshot = args.snapshot;
  printf("in raft stall rpc, snapshot is %s\n", persister_.snapshot.c_str());
  SaveRaftState();
  SaveSnapShot();

  lock.unlock();
  InstallSnapShotTokvServer();
  return reply;
}

std::vector<LogEntry> Raft::GetCmdAndTerm(std::string text) {
  std::vector<LogEntry> logs;
  size_t n = text.size();
  std::vector<std::string> strs;
  std::string tmp;
  for (size_t i = 0; i < n; i++) {
    if (text[i] != ';') {
      tmp += text[i];
    } else {
      if (!tmp.empty()) strs.push_back(tmp);
      tmp = "";
    }
  }
  for (auto & s : strs) {
    tmp = "";
    int j = 0;
    for (; j < s.size(); j++) {
      if (s[j] != ',') {
        tmp += s[j];
      } else
        break;
    }
    std::string number(s.begin() + j + 1, s.end());
    int num = std::stoi(number);
    logs.emplace_back(tmp, num);
  }
  return logs;
}

void Raft::PushBackLog(const LogEntry& log) { logs_.push_back(log); }

void Raft::SendAppendEntries() {
  buttonrpc client;
  AppendEntriesArgs args;
  int clientPeerId;
  std::unique_lock<std::mutex> lock(mutex_);

  // for(int i = 0; i < peers_.size(); i++){
  //     printf("in append %d's server.is_install_flag is %d\n", i,
  //     peers_[i].is_install_flag ? 1 : 0);
  // }

  for (int i = 0; i < peers_.size(); i++) {
    if (peers_[i].peer_id == peer_id_) continue;
    if (peers_[i].is_install_flag) continue;
    if (is_exist_index_.count(i)) continue;
    clientPeerId = i;
    is_exist_index_.insert(i);
    // printf("%d in append insert index : %d, size is %d\n", peer_id_, i,
    // is_exist_index_.size());
    break;
  }

  client.as_client("127.0.0.1", peers_[clientPeerId].port.second);
  // printf("%d send to %d's append port is %d\n", peer_id_, clientPeerId,
  // peers_[clientPeerId].port.second);

  if (is_exist_index_.size() == peers_.size() - 1) {
    // printf("append clear size is %d\n", is_exist_index_.size());
    for (auto& peer : peers_) {
      peer.is_install_flag = false;
    }
    is_exist_index_.clear();
  }

  args.term = curr_term_;
  args.leader_id = peer_id_;
  args.prev_log_index = next_index_[clientPeerId] - 1;
  args.leader_commit = commit_index_;

  for (int i = IdxToCompressLogPos(args.prev_log_index) + 1;
       i < logs_.size(); i++) {
    args.entrys += (logs_[i].command + "," +
                       std::to_string(logs_[i].term) + ";");
  }

  //用作自己调试可能，因为如果leader的m_prevLogIndex为0，follower的size必为0，自己调试直接赋日志给各个server看选举情况可能需要这段代码
  // if(args.prev_log_index == 0){
  //     args.prev_log_term = 0;
  //     if(logs_.size() != 0){
  //         args.prev_log_term = logs_[0].term;
  //     }
  // }

  if (args.prev_log_index == last_included_index_) {
    args.prev_log_term = last_included_term_;
  } else {  //有快照的话m_prevLogIndex必然不为0
    args.prev_log_term =
        logs_[IdxToCompressLogPos(args.prev_log_index)].term;
  }

  // printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n",
  // peer_id_, clientPeerId, args.prev_log_index, args.prev_log_term);

  lock.unlock();
  AppendEntriesReply reply =
      client.call<AppendEntriesReply>("AppendEntries", args).val();

  lock.lock();
  if (curr_term_ != args.term) {
    return;
  }
  if (reply.term > curr_term_) {
    state_ = FOLLOWER;
    curr_term_ = reply.term;
    voted_for_ = -1;
    SaveRaftState();
    return;  // FOLLOWER没必要维护nextIndex,成为leader会更新
  }

  if (reply.success) {
    next_index_[clientPeerId] = args.prev_log_index + static_cast<int>(GetCmdAndTerm(args.entrys).size()) + 1;  
  //可能RPC调用完log又增加了，但那些是不应该算进去的，不能直接取m_logs.size() + 1
    match_index_[clientPeerId] = next_index_[clientPeerId] - 1;
    match_index_[peer_id_] = LastIndex();

    std::vector<int> tmpIndex = match_index_;
    std::sort(tmpIndex.begin(), tmpIndex.end());
    int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
    if (realMajorityMatchIndex > commit_index_ &&
        (realMajorityMatchIndex <= last_included_index_ ||
         logs_[IdxToCompressLogPos(realMajorityMatchIndex)].term ==
             curr_term_)) {
      commit_index_ = realMajorityMatchIndex;
    }
  }

  if (!reply.success) {
    if (reply.conflict_term != -1 && reply.conflict_term != -100) {
      int leader_conflict_index = -1;
      for (int index = args.prev_log_index; index > last_included_index_;
           index--) {
        if (logs_[IdxToCompressLogPos(index)].term ==
            reply.conflict_term) {
          leader_conflict_index = index;
          break;
        }
      }
      if (leader_conflict_index != -1) {
        next_index_[clientPeerId] = leader_conflict_index + 1;
      } else {
        next_index_[clientPeerId] =
            reply
                .conflict_index;  //这里加不加1都可，无非是多一位还是少一位，此处指follower对应index为空
      }
    } else {
      if (reply.conflict_term == -100) {
      }
      //-------------------很关键，运行时不能注释下面这段，因为我自己调试bug强行增加bug，没有专门的测试程序-----------------
      else
        next_index_[clientPeerId] = reply.conflict_index;
    }
  }
  SaveRaftState();
}

AppendEntriesReply Raft::AppendEntries(const AppendEntriesArgs& args) {
  std::vector<LogEntry> recvLog = GetCmdAndTerm(args.entrys);
  AppendEntriesReply reply{};
  std::unique_lock<std::mutex> lock(mutex_);
  reply.term = curr_term_;
  reply.success = false;
  reply.conflict_index = -1;
  reply.conflict_term = -1;

  if (args.term < curr_term_) {
    return reply;
  }

  if (args.term >= curr_term_) {
    if (args.term > curr_term_) {
      voted_for_ = -1;
      SaveRaftState();
    }
    curr_term_ = args.term;
    state_ = FOLLOWER;
  }
  // printf("[%d] recv append from [%d] at self term%d, send term %d, duration
  // is %d\n",
  //         peer_id_, args.leader_id_, curr_term_, args.term,
  //         GetMyduration(last_wake_time_));
  ::gettimeofday(&last_wake_time_, nullptr);

  //------------------------------------test----------------------------------
  if (dead_) {
    reply.conflict_term = -100;
    return reply;
  }
  //------------------------------------test----------------------------------

  if (args.prev_log_index < last_included_index_) {
    printf("[%d]'s last_included_index_ is %d, but args.prev_log_index is %d\n",
           peer_id_, last_included_index_, args.prev_log_index);
    reply.conflict_index = 1;
    return reply;
  } else if (args.prev_log_index == last_included_index_) {
    printf("[%d]'s last_included_term_ is %d, args.prev_log_term is %d\n",
           peer_id_, last_included_term_, args.prev_log_term);
    if (args.prev_log_term !=
        last_included_term_) {  //脑裂分区，少数派的snapShot不对，回归集群后需要更新自己的snapShot及log
      reply.conflict_index = 1;
      return reply;
    }
  } else {
    if (LastIndex() < args.prev_log_index) {
      //索引要加1,很关键，避免快照安装一直循环(知道下次快照)，这里加不加1最多影响到回滚次数多一次还是少一次
      //如果不加1，先dead在activate，那么log的size一直都是lastincludedindx，next
      //= conflict = last一直循环， 知道下次超过maxstate，kvserver发起新快照才行
      reply.conflict_index = LastIndex() + 1;
      printf(
          " [%d]'s logs.size : %d < [%d]'s prevLogIdx : %d, ret conflict idx "
          "is %d\n",
          peer_id_, LastIndex(), args.leader_id, args.prev_log_index,
          reply.conflict_index);
      lock.unlock();
      reply.success = false;
      return reply;
    }
    //走到这里必然有日志，且prevLogIndex > 0
    if (logs_[IdxToCompressLogPos(args.prev_log_index)].term !=
        args.prev_log_term) {
      printf(" [%d]'s prevLogterm : %d != [%d]'s prevLogTerm : %d\n", peer_id_,
             logs_[IdxToCompressLogPos(args.prev_log_index)].term,
             args.leader_id, args.prev_log_term);

      reply.conflict_term =
          logs_[IdxToCompressLogPos(args.prev_log_index)].term;
      for (int index = last_included_index_ + 1; index <= args.prev_log_index;
           index++) {
        if (logs_[IdxToCompressLogPos(index)].term == reply.conflict_term) {
          reply.conflict_index = index;  //找到冲突term的第一个index,比索引要加1
          break;
        }
      }
      lock.unlock();
      reply.success = false;
      return reply;
    }
  }
  //走到这里必然PrevLogterm与对应follower的index处term相等，进行日志覆盖
  int logSize = LastIndex();
  for (int i = args.prev_log_index; i < logSize; i++) {
    logs_.pop_back();
  }
  // logs_.insert(logs_.end(), recvLog.begin(), recvLog.end());
  for (const auto& log : recvLog) {
    PushBackLog(log);
  }
  SaveRaftState();
  if (commit_index_ < args.leader_commit) {
    commit_index_ = std::min(args.leader_commit, LastIndex());
    // commit_index_ = args.leader_commit;
  }
  // for(auto a : logs_) printf("%d ", a.term);
  // printf(" [%d] sync success\n", peer_id_);
  lock.unlock();
  reply.success = true;
  return reply;
}

std::pair<int, bool> Raft::GetState() {
  std::pair<int, bool> serverState;
  serverState.first = curr_term_;
  serverState.second = (state_ == LEADER);
  return serverState;
}

void Raft::Kill() {
  dead_ = true;
  printf("raft%d is dead\n", peer_id_);
}

void Raft::Activate() {
  dead_ = false;
  printf("raft%d is Activate\n", peer_id_);
}

StartRet Raft::Start(const Operation& op) {
  StartRet ret;
  std::unique_lock<std::mutex> lock(mutex_);
  RAFT_STATE state = state_;
  if (state != LEADER) {
    // printf("index : %d, term : %d, isleader : %d\n", ret.cmd_index,
    // ret.curr_term_, ret.is_leader == false ? 0 : 1);
    return ret;
  }

  LogEntry log;
  log.command = op.GetCmd();
  log.term = curr_term_;
  PushBackLog(log);

  ret.cmd_index = LastIndex();
  ret.curr_term = curr_term_;
  ret.is_leader = true;
  // printf("index : %d, term : %d, isleader : %d\n", ret.cmd_index,
  // ret.curr_term_, ret.is_leader == false ? 0 : 1);

  return ret;
}

void Raft::PrintLogs() {
  for (const auto& a : logs_) {
    printf("logs : %d\n", a.term);
  }
  std::cout << std::endl;
}

void Raft::Serialize() {
  std::string str;
  str += std::to_string(this->persister_.curr_term) + ";" +
         std::to_string(this->persister_.voted_for) + ";";
  str += std::to_string(this->persister_.last_included_index) + ";" +
         std::to_string(this->persister_.last_included_term) + ";\n";
  for (const auto& log : this->persister_.logs) {
    str += log.command + "," + std::to_string(log.term) + "\n";
  }
  std::string filename = "persister_-" + std::to_string(peer_id_);
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    std::perror("open");
    std::exit(-1);
  }
  ssize_t len = ::write(fd, str.c_str(), str.size());
  if (len == -1) {
    std::perror("write");
    ::close(fd);  //无论如何都需要关闭文件，所以这里也需要关闭。
    std::exit(-1);
  }
  ::close(fd);
}

bool Raft::Deserialize() {
  std::string filename = "persister_-" + std::to_string(peer_id_);
  if (::access(filename.c_str(), F_OK) == -1) return false;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    std::perror("open");
    return false;
  }
  ssize_t length = ::lseek(fd, 0, SEEK_END);
  ::lseek(fd, 0, SEEK_SET);
  char buf[length];
  ::bzero(buf, length);
  ssize_t len = ::read(fd, buf, length);
  if (len != length) {
    std::perror("read");
    ::close(fd); //无论如何都需要关闭文件，所以这里也需要关闭
    std::exit(-1);
  }
  ::close(fd);
  std::string content(buf);
  std::vector<std::string> persist;
  std::string tmp;
  for (char c : content) {
    if (c != ';') {
      tmp += c;
    } else {
      if (!tmp.empty()) persist.push_back(tmp);
      tmp = "";
    }
  }
  persist.push_back(tmp);
  this->persister_.curr_term = std::stoi(persist[0]);
  this->persister_.voted_for = std::stoi(persist[1]);
  this->persister_.last_included_index = std::stoi(persist[2]);
  this->persister_.last_included_term = std::stoi(persist[3]);
  std::vector<std::string> logs_str;
  std::vector<LogEntry> logs;
  tmp = "";
  for (int i = 1; i < persist[4].size(); i++) {
    if (persist[4][i] != '\n') {
      tmp += persist[4][i];
    } else {
      if (!tmp.empty()) logs_str.push_back(tmp);
      tmp = "";
    }
  }
  for (auto& log_str : logs_str) {
    tmp = "";
    int j = 0;
    for (; j < log_str.size(); j++) {
      if (log_str[j] != ',') {
        tmp += log_str[j];
      } else
        break;
    }
    std::string number(log_str.begin() + j + 1, log_str.end());
    int num = std::stoi(number);
    logs.emplace_back(tmp, num);
  }
  this->persister_.logs = logs;
  return true;
}

void Raft::ReadRaftState() {
  //只在初始化的时候调用，没必要加锁，因为run()在其之后才执行
  bool ret = this->Deserialize();
  if (!ret) return;
  this->curr_term_ = this->persister_.curr_term;
  this->voted_for_ = this->persister_.voted_for;

  for (const auto& log : this->persister_.logs) {
    PushBackLog(log);
  }
  printf(" [%d]'s term : %d, votefor : %d, logs.size() : %d\n", peer_id_,
         curr_term_, voted_for_, static_cast<int>(logs_.size()));
}

void Raft::SaveRaftState() {
  persister_.curr_term = curr_term_;
  persister_.voted_for = voted_for_;
  persister_.logs = logs_;
  persister_.last_included_index = this->last_included_index_;
  persister_.last_included_term = this->last_included_term_;
  Serialize();
}

void Raft::SetSendSem(int num) { send_sem_.Init(num); }
void Raft::SetRecvSem(int num) { recv_sem_.Init(num); }

bool Raft::WaitSendSem() { return send_sem_.Wait(); }
bool Raft::WaitRecvSem() { return recv_sem_.Wait(); }
bool Raft::PostSendSem() { return send_sem_.Post(); }
bool Raft::PostRecvSem() { return recv_sem_.Post(); }

ApplyMsg Raft::GetBackMsg() { return msgs_.back(); }

bool Raft::ExceedLogSize(int size) {
  bool ret = false;

  std::unique_lock<std::mutex> lock(mutex_);
  int sum = 8;
  for (auto& log : persister_.logs) {
    sum += static_cast<int>(log.command.size()) + 3;
  }
  ret = (sum >= size ? true : false);
  if (ret) printf("[%d] in Exceed the log size is %d\n", peer_id_, sum);

  return ret;
}

void Raft::RecvSnapShot(std::string snap_shot, int last_included_index) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (last_included_index < this->last_included_index_) {
    return;
  }
  int compressLen = last_included_index - this->last_included_index_;
  printf(
      "[%d] before log.size is %d, compressLen is %d, last_included_index is "
      "%d\n",
      peer_id_, static_cast<int>(logs_.size()), compressLen, last_included_index_);

  printf("[%d] : %d - %d = compressLen is %d\n", peer_id_, last_included_index,
         this->last_included_index_, compressLen);
  this->last_included_term_ =
      logs_[IdxToCompressLogPos(last_included_index)].term;
  this->last_included_index_ = last_included_index;

  std::vector<LogEntry> tmpLogs;
  for (int i = compressLen; i < logs_.size(); i++) {
    tmpLogs.push_back(logs_[i]);
  }
  logs_ = tmpLogs;
  printf("[%d] after log.size is %d\n", peer_id_, static_cast<int>(logs_.size()));
  //更新了logs及lastTerm和lastIndex，需要持久化
  persister_.snapshot = std::move(snap_shot);
  SaveRaftState();

  SaveSnapShot();
  printf("[%d] persister_.size is %d, last_included_index is %d\n", peer_id_,
         static_cast<int>(persister_.logs.size()), last_included_index_);
}

int Raft::IdxToCompressLogPos(int index) const {
  return index - last_included_index_ - 1;
}

bool Raft::ReadSnapShot() {
  std::string filename = "snap_shot-" + std::to_string(peer_id_);
  if (::access(filename.c_str(), F_OK) == -1) return false;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    std::perror("::open");
    return false;
  }
  ssize_t length = ::lseek(fd, 0, SEEK_END);
  ::lseek(fd, 0, SEEK_SET);
  char buf[length];
  ::bzero(buf, length);
  ssize_t len = ::read(fd, buf, length);
  if (len != length) {
    std::perror("::read");
    std::exit(-1);
  }
  ::close(fd);
  std::string snap_shot(buf);
  persister_.snapshot = snap_shot;
  return true;
}

void Raft::SaveSnapShot() const {
  std::string filename = "snap_shot-" + std::to_string(peer_id_);
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    std::perror("::open");
    std::exit(-1);
  }
  ssize_t len = ::write(fd, persister_.snapshot.c_str(),
                    persister_.snapshot.size() + 1);
  ::close(fd);
}

void Raft::InstallSnapShotTokvServer() {
  // while(true){
  //   std::unique_lock<std::mutex> lock(mutex_);
  //   installSnapShotFlag = true;
  //   printf("%d install to kvserver, install is %d but apply is %d\n",
  //          peer_id_, installSnapShotFlag ? 1 : 0, applyLogFlag ? 1 : 0);
  //   if(applyLogFlag){
  //      lock.unlock();
  //      ::usleep(1000);
  //      continue;
  //   }
  //   break;
  // }
  std::unique_lock<std::mutex> lock(mutex_);
  bool ret = ReadSnapShot();

  if (!ret) {
    // installSnapShotFlag = false;
    // applyLogFlag = true;
    return;
  }

  ApplyMsg msg;
  msg.is_command_valid = false;
  msg.snapshot = persister_.snapshot;
  msg.last_included_index = this->last_included_index_;
  msg.last_included_term = this->last_included_term_;

  last_applied_ = last_included_index_;
  lock.unlock();

  WaitRecvSem();
  msgs_.push_back(msg);
  PostSendSem();

  printf("%d call install RPC\n", peer_id_);
}

int Raft::LastIndex() { return last_included_index_ + static_cast<int>(logs_.size()); }

int Raft::LastTerm() {
  int LastTerm = last_included_term_;
  if (!logs_.empty()) {
    LastTerm = logs_.back().term;
  }
  return LastTerm;
}

#if 0 
int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("loss parameter of peersNum\n");
    std::exit(-1);
  }
  int peers_num = std::atoi(argv[1]);
  if (peers_num % 2 == 0) {
    printf("the peersNum should be odd\n");
    std::exit(-1);
  }
  std::srand((unsigned)std::time(nullptr));
  std::vector<PeersInfo> peers(peers_num);
  for (int i = 0; i < peers_num; i++) {
    peers[i].peer_id = i;
    peers[i].port.first = COMMOM_PORT + i;
    peers[i].port.second = COMMOM_PORT + i + peers.size();
    // printf(" id : %d port1 : %d, port2 : %d\n", peers[i].peer_id_,
    // peers[i].port.first, peers[i].port.second);
  }

  std::vector<std::unique_ptr<Raft>> rafts;
  rafts.reserve(peers_num);
  // Raft* rafts = new Raft[peers.size()];
  for (int i = 0; i < peers_num; ++i) {
    rafts.emplace_back(std::make_unique<Raft>());
    rafts[i]->Make(peers, i);
  }

  ::usleep(400000);
  for (int i = 0; i < peers.size(); i++) {
    if (rafts[i]->GetState().second) {
      for (int j = 0; j < 1000; j++) {
        Operation opera;
        opera.op = "put";
        opera.key = std::to_string(j);
        opera.value = std::to_string(j);
        rafts[i]->Start(opera);
        ::usleep(50000);
      }
    } else
      continue;
  }
  ::usleep(400000);
  for (int i = 0; i < peers.size(); i++) {
    if (rafts[i]->GetState().second) {
      rafts[i]->Kill();
      break;
    }
  }

  while (1)
    ;
}
#endif  //#if 0

#endif  // RYAN_DS_KV_RAFT_RAFT_H_