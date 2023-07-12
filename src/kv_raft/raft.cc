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
#include <vector>

#include "buttonrpc.hpp"
#include "semaphore.hpp"

constexpr int COMMOM_PORT = 1234;
constexpr int HEART_BEART_PERIOD = 100000;

/**
 * @brief
 * 修改挺大的，注释过的不再注释了，看LAB2Braft.cpp的注释，增加了同kvServer应用层交互的代码，以及处理应用层快照的逻辑
 * 新增了installSnapShotRPC，即在心跳中除了append分支还多了安装快照的分支，由于快照会截断日志，所以原先和日志长度、索引等有关的逻辑全得重新改
 * 同时由于C++和go的差异，许多协程能实现的地方需要多很多同步信息，需要重新设置关于appendLoop中append和install的RPC端口信息以及对应客户端关系
 * 直接看raft的类定义，里面对新增的函数及成员做了简单注释
 */

//新增的快照RPC需要传的参数，具体看论文section7关于日志压缩的内容
struct InstallSnapShotArgs {
  int term;
  int leader_id;
  int last_included_index;
  int last_included_term;
  std::string snapshot;

  friend Serializer& operator>>(Serializer& in, InstallSnapShotArgs& d) {
    in >> d.term >> d.leader_id >> d.last_included_index >>
        d.last_included_term >> d.snapshot;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, InstallSnapShotArgs d) {
    out << d.term << d.leader_id << d.last_included_index
        << d.last_included_term << d.snapshot;
    return out;
  }
};

struct InstallSnapSHotReply {
  int term;
};

struct Operation {
  std::string GetCmd();
  std::string op;
  std::string key;
  std::string value;
  int client_id;
  int request_id;
  int term;
  int index;
};

std::string Operation::GetCmd() {
  std::string cmd = op + " " + key + " " + value + " " +
                    std::to_string(client_id) + " " +
                    std::to_string(request_id);
  return cmd;
}

struct StartRet {
  StartRet() : cmd_index(-1), curr_term_(-1), is_leader(false) {}
  int cmd_index;
  int curr_term_;
  bool is_leader;
};

struct ApplyMsg {
  bool is_command_valid;
  std::string command;
  int command_index;
  int command_term;
  Operation GetOperation();

  int last_included_index;
  int last_included_term;
  std::string snap_shot;
};

Operation ApplyMsg::GetOperation() {
  Operation operation;
  std::vector<std::string> str;
  std::string tmp;
  for (int i = 0; i < command.size(); i++) {
    if (command[i] != ' ') {
      tmp += command[i];
    } else {
      if (tmp.size() != 0) str.push_back(tmp);
      tmp = "";
    }
  }
  if (tmp.size() != 0) {
    str.push_back(tmp);
  }
  operation.op = str[0];
  operation.key = str[1];
  operation.value = str[2];
  operation.client_id = std::atoi(str[3].c_str());
  operation.request_id = std::atoi(str[4].c_str());
  operation.term = command_term;
  return operation;
}

struct PeersInfo {
  std::pair<int, int> port;
  int peer_id;
  bool is_install_flag;
};

struct LogEntry {
  LogEntry(std::string cmd = "", int term = -1)
      : command_str(cmd), term(term) {}
  std::string command_str;
  int term;
};

struct Persister {
  std::vector<LogEntry> logs;
  std::string snap_shot;
  int curr_term;
  int voted_for;
  int last_included_index;
  int last_included_term;
};

class AppendEntriesArgs {
 public:
  // AppendEntriesArgs():term(-1), leader_id_(-1), prev_log_index(-1),
  // prev_log_term(-1){
  //     //leader_commit = 0;
  //     send_logs.clear();
  // }
  int term;
  int leader_id_;
  int prev_log_index;
  int prev_log_term;
  int leader_commit;
  std::string send_logs;
  friend Serializer& operator>>(Serializer& in, AppendEntriesArgs& d) {
    in >> d.term >> d.leader_id_ >> d.prev_log_index >> d.prev_log_term >>
        d.leader_commit >> d.send_logs;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, AppendEntriesArgs d) {
    out << d.term << d.leader_id_ << d.prev_log_index << d.prev_log_term
        << d.leader_commit << d.send_logs;
    return out;
  }
};

class AppendEntriesReply {
 public:
  int term;
  bool success;
  int conflict_term;
  int conflict_index;
};

class RequestVoteArgs {
 public:
  int term;
  int candidate_id;
  int last_log_term;
  int last_log_index;
};

class RequestVoteReply {
 public:
  int term;
  bool vote_granted;
};

class Raft {
 public:
  static void* ListenForVote(void* arg);
  static void* ListenForAppend(void* arg);
  static void* ProcessEntriesLoop(void* arg);
  static void* ElectionLoop(void* arg);
  static void* CallRequestVote(void* arg);
  static void* SendAppendEntries(
      void* arg);  //向其他follower发送快照的函数，处理逻辑看论文
  static void* SendInstallSnapShot(void* arg);
  static void* ApplyLogLoop(void* arg);

  enum RAFT_STATE { LEADER = 0, CANDIDATE, FOLLOWER };
  void Make(std::vector<PeersInfo> peers, int id);
  int GetMyduration(timeval last);
  void SetBroadcastTime();
  std::pair<int, bool> GetState();
  RequestVoteReply RequestVote(RequestVoteArgs args);
  AppendEntriesReply AppendEntries(AppendEntriesArgs args);
  InstallSnapSHotReply InstallSnapShot(
      InstallSnapShotArgs args);  //安装快照的RPChandler，处理逻辑看论文
  bool CheckLogUptodate(int term, int index);
  void PushBackLog(LogEntry log);
  std::vector<LogEntry> GetCmdAndTerm(std::string text);
  StartRet Start(Operation op);
  void PrintLogs();
  void SetSendSem(
      int num);  //初始化send的信号量，结合kvServer层的有名管道fifo模拟go的select及channel
  void SetRecvSem(
      int num);  //初始化recv的信号量，结合kvServer层的有名管道fifo模拟go的select及channel
  bool WaitSendSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  bool WaitRecvSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  bool PostSendSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  bool PostRecvSem();  //信号量函数封装，用于类复合时kvServer的类外调用
  ApplyMsg
  GetBackMsg();  //取得一个msg，结合信号量和fifo模拟go的select及channel，每次只取一个，处理完再取

  void Serialize();
  bool Deserialize();
  void SaveRaftState();
  void ReadRaftState();
  bool IsKilled();  //->check is killed?
  void Kill();
  void Activate();

  bool ExceedLogSize(
      int size);  //超出日志大小则需要快照，kvServer层需要有个守护线程持续调用该函数判断
  void RecvSnapShot(
      std::string snap_shot,
      int last_included_index);  //接受来自kvServer层的快照，用于持久化
  int IdxToCompressLogPos(int index);  //获得原先索引在截断日志后的索引
  bool ReadSnapShot();                 //读取快照
  void SaveSnapShot();                 //持久化快照
  void InstallSnapShotTokvServer();  //落后的raft向对应的应用层安装快照
  int LastIndex();                   //截断日志后的lastIndex
  int LastTerm();                    //截断日志后的lastTerm

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  std::vector<PeersInfo> peers_;
  Persister persister_;
  int peer_id_;
  int dead_;

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

  // unordered_map<int, int> m_firstIndexOfEachTerm;
  // std::vector<int> next_index_;
  // std::vector<int> match_index_;

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

void Raft::Make(std::vector<PeersInfo> peers, int id) {
  peers_ = peers;
  // this->persister_ = persister_;
  peer_id_ = id;
  dead_ = 0;

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

  // pthread_t listen_tid1;
  // pthread_t listen_tid2;
  // pthread_t listen_tid3;
  // pthread_create(&listen_tid1, nullptr, ListenForVote, this);
  // pthread_detach(listen_tid1);
  // pthread_create(&listen_tid2, nullptr, ListenForAppend, this);
  // pthread_detach(listen_tid2);
  // pthread_create(&listen_tid3, nullptr, ApplyLogLoop, this);
  // pthread_detach(listen_tid3);
  std::thread(&Raft::ListenForVote, this).detach();
  std::thread(&Raft::ListenForAppend, this).detach();
  std::thread(&Raft::ApplyLogLoop, this).detach();

  // Using std::thread instead of pthread
  // std::thread listen_thread1(&Raft::ListenForVote, this);
  // listen_thread1.detach();
  // std::thread listen_thread2(&Raft::ListenForAppend, this);
  // listen_thread2.detach();
  // std::thread listen_thread3(&Raft::ApplyLogLoop, this);
  // listen_thread3.detach();
}

void* Raft::ApplyLogLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  while (true) {
    while (!raft->dead_) {
      // raft->m_lock.lock();
      // if(raft->installSnapShotFlag){
      //     printf("%d check install : %d, apply : %d\n", raft->peer_id_,
      //         raft->installSnapShotFlag? 1 : 0, raft->applyLogFlag ? 1 : 0);
      //     raft->applyLogFlag = false;
      //     raft->m_lock.unlock();
      //     ::usleep(10000);
      //     continue;
      // }
      // raft->m_lock.unlock();
      ::usleep(10000);
      // printf("%d's apply is called, apply is %d, commit is %d\n",
      // raft->peer_id_, raft->last_applied_, raft->commit_index_);
      std::vector<ApplyMsg> msgs;
      std::unique_lock<std::mutex> lock(raft->mutex_);
      while (raft->last_applied_ < raft->commit_index_) {
        raft->last_applied_++;
        int appliedIdx = raft->IdxToCompressLogPos(raft->last_applied_);
        ApplyMsg msg;
        msg.command = raft->logs_[appliedIdx].command_str;
        msg.is_command_valid = true;
        msg.command_term = raft->logs_[appliedIdx].term;
        msg.command_index = raft->last_applied_;
        msgs.push_back(msg);
      }
      lock.unlock();
      for (int i = 0; i < msgs.size(); i++) {
        // printf("before %d's apply is called, apply is %d, commit is %d\n",
        //     raft->peer_id_, raft->last_applied_, raft->commit_index_);
        raft->WaitRecvSem();
        // printf("after %d's apply is called, apply is %d, commit is %d\n",
        //     raft->peer_id_, raft->last_applied_, raft->commit_index_);
        raft->msgs_.push_back(msgs[i]);
        raft->PostSendSem();
      }
    }
    ::usleep(10000);
  }
}

int Raft::GetMyduration(timeval last) {
  struct timeval now;
  ::gettimeofday(&now, nullptr);
  // printf("--------------------------------\n");
  // printf("now's sec : %ld, now's usec : %ld\n", now.tv_sec, now.tv_usec);
  // printf("last's sec : %ld, last's usec : %ld\n", last.tv_sec, last.tv_usec);
  // printf("%d\n", ((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec -
  // last.tv_usec))); printf("--------------------------------\n");
  return ((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec - last.tv_usec));
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

void* Raft::ListenForVote(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc server;
  server.as_server(raft->peers_[raft->peer_id_].port.first);
  server.bind("RequestVote", &Raft::RequestVote, raft);

  std::thread(&Raft::ElectionLoop, raft).detach();

  server.run();
  printf("std::exit!\n");
}

void* Raft::ListenForAppend(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc server;
  server.as_server(raft->peers_[raft->peer_id_].port.second);
  server.bind("AppendEntries", &Raft::AppendEntries, raft);
  server.bind("InstallSnapShot", &Raft::InstallSnapShot, raft);

  std::thread(&Raft::ProcessEntriesLoop, raft).detach();

  server.run();
  printf("std::exit!\n");
}

void* Raft::ElectionLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  bool resetFlag = false;
  while (!raft->dead_) {
    int timeOut = std::rand() % 200000 + 200000;
    while (1) {
      ::usleep(1000);
      std::unique_lock<std::mutex> lock(raft->mutex_);

      int during_time = raft->GetMyduration(raft->last_wake_time_);
      if (raft->state_ == FOLLOWER && during_time > timeOut) {
        raft->state_ = CANDIDATE;
      }

      if (raft->state_ == CANDIDATE && during_time > timeOut) {
        printf(" %d attempt election at term %d, timeOut is %d\n",
               raft->peer_id_, raft->curr_term_, timeOut);
        ::gettimeofday(&raft->last_wake_time_, nullptr);
        resetFlag = true;
        raft->curr_term_++;
        raft->voted_for_ = raft->peer_id_;
        raft->SaveRaftState();

        raft->recv_votes_ = 1;
        raft->finished_vote_ = 1;
        raft->curr_peer_id_ = 0;

        for (auto server : raft->peers_) {
          if (server.peer_id == raft->peer_id_) continue;
          std::thread(&Raft::CallRequestVote, raft).detach();
        }

        while (raft->recv_votes_ <= raft->peers_.size() / 2 &&
               raft->finished_vote_ != raft->peers_.size()) {
          raft->cond_.wait(lock);
        }
        if (raft->state_ != CANDIDATE) {
          continue;
        }
        if (raft->recv_votes_ > raft->peers_.size() / 2) {
          raft->state_ = LEADER;

          for (int i = 0; i < raft->peers_.size(); i++) {
            raft->next_index_[i] = raft->LastIndex() + 1;
            raft->match_index_[i] = 0;
          }

          printf(" %d become new leader at term %d\n", raft->peer_id_,
                 raft->curr_term_);
          raft->SetBroadcastTime();
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

void* Raft::CallRequestVote(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc client;
  std::unique_lock<std::mutex> lock(raft->mutex_);
  RequestVoteArgs args;
  args.candidate_id = raft->peer_id_;
  args.term = raft->curr_term_;
  args.last_log_index = raft->LastIndex();
  args.last_log_term = raft->LastTerm();
  if (raft->curr_peer_id_ == raft->peer_id_) {
    raft->curr_peer_id_++;
  }
  int clientPeerId = raft->curr_peer_id_;
  client.as_client("127.0.0.1", raft->peers_[raft->curr_peer_id_++].port.first);

  if (raft->curr_peer_id_ == raft->peers_.size() ||
      (raft->curr_peer_id_ == raft->peers_.size() - 1 &&
       raft->peer_id_ == raft->curr_peer_id_)) {
    raft->curr_peer_id_ = 0;
  }
  lock.unlock();

  RequestVoteReply reply =
      client.call<RequestVoteReply>("RequestVote", args).val();
  lock.lock();
  raft->finished_vote_++;
  raft->cond_.notify_one();
  if (reply.term > raft->curr_term_) {
    raft->state_ = FOLLOWER;
    raft->curr_term_ = reply.term;
    raft->voted_for_ = -1;
    raft->ReadRaftState();
    return nullptr;
  }
  if (reply.vote_granted) {
    raft->recv_votes_++;
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
  RequestVoteReply reply;
  reply.vote_granted = false;
  std::unique_lock<std::mutex> lock(mutex_);
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

void* Raft::ProcessEntriesLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  while (!raft->dead_) {
    ::usleep(1000);
    std::unique_lock<std::mutex> lock(raft->mutex_);
    if (raft->state_ != LEADER) {
      continue;
    }
    // printf("sec : %ld, usec : %ld\n", raft->last_broadcast_time_.tv_sec,
    // raft->last_broadcast_time_.tv_usec);
    int during_time = raft->GetMyduration(raft->last_broadcast_time_);
    // printf("time is %d\n", during_time);
    if (during_time < HEART_BEART_PERIOD) {
      continue;
    }

    ::gettimeofday(&raft->last_broadcast_time_, nullptr);
    // printf("%d send AppendRetries at %d\n", raft->peer_id_,
    // raft->curr_term_); raft->m_lock.unlock();

    for (auto& server :
         raft->peers_) {  // TODO: 严重警告，此处不能加const，因为要修改
      if (server.peer_id == raft->peer_id_) continue;
      if (raft->next_index_[server.peer_id] <=
          raft->last_included_index_) {  //进入install分支的条件，日志落后于leader的快照
        printf(
            "%d send install rpc to %d, whose nextIdx is %d, but leader's "
            "lastincludeIdx is %d\n",
            raft->peer_id_, server.peer_id, raft->next_index_[server.peer_id],
            raft->last_included_index_);
        server.is_install_flag = true;
        std::thread(&Raft::SendInstallSnapShot, raft).detach();
      } else {
        printf(
            "%d send append rpc to %d, whose nextIdx is %d, but leader's "
            "lastincludeIdx is %d\n",
            raft->peer_id_, server.peer_id, raft->next_index_[server.peer_id],
            raft->last_included_index_);
        std::thread(&Raft::SendAppendEntries, raft).detach();
      }
    }
  }
}

void* Raft::SendInstallSnapShot(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc client;
  InstallSnapShotArgs args;
  int clientPeerId;
  std::unique_lock<std::mutex> lock(raft->mutex_);
  // for(int i = 0; i < raft->peers_.size(); i++){
  //     printf("in install %d's server.is_install_flag is %d\n", i,
  //     raft->peers_[i].is_install_flag ? 1 : 0);
  // }
  for (int i = 0; i < raft->peers_.size(); i++) {
    if (raft->peers_[i].peer_id == raft->peer_id_) {
      // printf("%d is leader, continue\n", i);
      continue;
    }
    if (!raft->peers_[i].is_install_flag) {
      // printf("%d is append, continue\n", i);
      continue;
    }
    if (raft->is_exist_index_.count(i)) {
      // printf("%d is chongfu, continue\n", i);
      continue;
    }
    clientPeerId = i;
    raft->is_exist_index_.insert(i);
    // printf("%d in install insert index : %d, size is %d\n", raft->peer_id_,
    // i, raft->is_exist_index_.size());
    break;
  }

  client.as_client("127.0.0.1", raft->peers_[clientPeerId].port.second);

  if (raft->is_exist_index_.size() == raft->peers_.size() - 1) {
    // printf("install clear size is %d\n", raft->is_exist_index_.size());
    for (int i = 0; i < raft->peers_.size(); i++) {
      raft->peers_[i].is_install_flag = false;
    }
    raft->is_exist_index_.clear();
  }

  args.last_included_index = raft->last_included_index_;
  args.last_included_term = raft->last_included_term_;
  args.leader_id = raft->peer_id_;
  args.term = raft->curr_term_;
  raft->ReadSnapShot();
  args.snapshot = raft->persister_.snap_shot;

  printf("in send install snap_shot is %s\n", args.snapshot.c_str());

  lock.unlock();
  // printf("%d send to %d's install port is %d\n", raft->peer_id_,
  // clientPeerId, raft->peers_[clientPeerId].port.second);
  InstallSnapSHotReply reply =
      client.call<InstallSnapSHotReply>("InstallSnapShot", args).val();
  // printf("%d is called send install to %d\n", raft->peer_id_, clientPeerId);

  lock.lock();
  if (raft->curr_term_ != args.term) {
    return nullptr;
  }

  if (raft->curr_term_ < reply.term) {
    raft->state_ = FOLLOWER;
    raft->voted_for_ = -1;
    raft->curr_term_ = reply.term;
    raft->SaveRaftState();
    return nullptr;
  }

  raft->next_index_[clientPeerId] = raft->LastIndex() + 1;
  raft->match_index_[clientPeerId] = args.last_included_index;

  raft->match_index_[raft->peer_id_] = raft->LastIndex();
  std::vector<int> tmpIndex = raft->match_index_;
  std::sort(tmpIndex.begin(), tmpIndex.end());
  int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
  if (realMajorityMatchIndex > raft->commit_index_ &&
      (realMajorityMatchIndex <= raft->last_included_index_ ||
       raft->logs_[raft->IdxToCompressLogPos(realMajorityMatchIndex)].term ==
           raft->curr_term_)) {
    raft->commit_index_ = realMajorityMatchIndex;
  }
}

InstallSnapSHotReply Raft::InstallSnapShot(InstallSnapShotArgs args) {
  InstallSnapSHotReply reply;
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
  persister_.snap_shot = args.snapshot;
  printf("in raft stall rpc, snap_shot is %s\n", persister_.snap_shot.c_str());
  SaveRaftState();
  SaveSnapShot();

  lock.unlock();
  InstallSnapShotTokvServer();
  return reply;
}

std::vector<LogEntry> Raft::GetCmdAndTerm(std::string text) {
  std::vector<LogEntry> logs;
  int n = text.size();
  std::vector<std::string> str;
  std::string tmp = "";
  for (int i = 0; i < n; i++) {
    if (text[i] != ';') {
      tmp += text[i];
    } else {
      if (tmp.size() != 0) str.push_back(tmp);
      tmp = "";
    }
  }
  for (int i = 0; i < str.size(); i++) {
    tmp = "";
    int j = 0;
    for (; j < str[i].size(); j++) {
      if (str[i][j] != ',') {
        tmp += str[i][j];
      } else
        break;
    }
    std::string number(str[i].begin() + j + 1, str[i].end());
    int num = std::atoi(number.c_str());
    logs.push_back(LogEntry(tmp, num));
  }
  return logs;
}

void Raft::PushBackLog(LogEntry log) { logs_.push_back(log); }

void* Raft::SendAppendEntries(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);

  buttonrpc client;
  AppendEntriesArgs args;
  int clientPeerId;
  std::unique_lock<std::mutex> lock(raft->mutex_);

  // for(int i = 0; i < raft->peers_.size(); i++){
  //     printf("in append %d's server.is_install_flag is %d\n", i,
  //     raft->peers_[i].is_install_flag ? 1 : 0);
  // }

  for (int i = 0; i < raft->peers_.size(); i++) {
    if (raft->peers_[i].peer_id == raft->peer_id_) continue;
    if (raft->peers_[i].is_install_flag) continue;
    if (raft->is_exist_index_.count(i)) continue;
    clientPeerId = i;
    raft->is_exist_index_.insert(i);
    // printf("%d in append insert index : %d, size is %d\n", raft->peer_id_, i,
    // raft->is_exist_index_.size());
    break;
  }

  client.as_client("127.0.0.1", raft->peers_[clientPeerId].port.second);
  // printf("%d send to %d's append port is %d\n", raft->peer_id_, clientPeerId,
  // raft->peers_[clientPeerId].port.second);

  if (raft->is_exist_index_.size() == raft->peers_.size() - 1) {
    // printf("append clear size is %d\n", raft->is_exist_index_.size());
    for (int i = 0; i < raft->peers_.size(); i++) {
      raft->peers_[i].is_install_flag = false;
    }
    raft->is_exist_index_.clear();
  }

  args.term = raft->curr_term_;
  args.leader_id_ = raft->peer_id_;
  args.prev_log_index = raft->next_index_[clientPeerId] - 1;
  args.leader_commit = raft->commit_index_;

  for (int i = raft->IdxToCompressLogPos(args.prev_log_index) + 1;
       i < raft->logs_.size(); i++) {
    args.send_logs += (raft->logs_[i].command_str + "," +
                       std::to_string(raft->logs_[i].term) + ";");
  }

  //用作自己调试可能，因为如果leader的m_prevLogIndex为0，follower的size必为0，自己调试直接赋日志给各个server看选举情况可能需要这段代码
  // if(args.prev_log_index == 0){
  //     args.prev_log_term = 0;
  //     if(raft->logs_.size() != 0){
  //         args.prev_log_term = raft->logs_[0].term;
  //     }
  // }

  if (args.prev_log_index == raft->last_included_index_) {
    args.prev_log_term = raft->last_included_term_;
  } else {  //有快照的话m_prevLogIndex必然不为0
    args.prev_log_term =
        raft->logs_[raft->IdxToCompressLogPos(args.prev_log_index)].term;
  }

  // printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n",
  // raft->peer_id_, clientPeerId, args.prev_log_index, args.prev_log_term);

  lock.unlock();
  AppendEntriesReply reply =
      client.call<AppendEntriesReply>("AppendEntries", args).val();

  lock.lock();
  if (raft->curr_term_ != args.term) {
    return nullptr;
  }
  if (reply.term > raft->curr_term_) {
    raft->state_ = FOLLOWER;
    raft->curr_term_ = reply.term;
    raft->voted_for_ = -1;
    raft->SaveRaftState();
    return nullptr;  // FOLLOWER没必要维护nextIndex,成为leader会更新
  }

  if (reply.success) {
    raft->next_index_[clientPeerId] =
        args.prev_log_index + raft->GetCmdAndTerm(args.send_logs).size() +
        1;  //可能RPC调用完log又增加了，但那些是不应该算进去的，不能直接取m_logs.size()
            //+ 1
    raft->match_index_[clientPeerId] = raft->next_index_[clientPeerId] - 1;
    raft->match_index_[raft->peer_id_] = raft->LastIndex();

    std::vector<int> tmpIndex = raft->match_index_;
    std::sort(tmpIndex.begin(), tmpIndex.end());
    int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
    if (realMajorityMatchIndex > raft->commit_index_ &&
        (realMajorityMatchIndex <= raft->last_included_index_ ||
         raft->logs_[raft->IdxToCompressLogPos(realMajorityMatchIndex)].term ==
             raft->curr_term_)) {
      raft->commit_index_ = realMajorityMatchIndex;
    }
  }

  if (!reply.success) {
    if (reply.conflict_term != -1 && reply.conflict_term != -100) {
      int leader_conflict_index = -1;
      for (int index = args.prev_log_index; index > raft->last_included_index_;
           index--) {
        if (raft->logs_[raft->IdxToCompressLogPos(index)].term ==
            reply.conflict_term) {
          leader_conflict_index = index;
          break;
        }
      }
      if (leader_conflict_index != -1) {
        raft->next_index_[clientPeerId] = leader_conflict_index + 1;
      } else {
        raft->next_index_[clientPeerId] =
            reply
                .conflict_index;  //这里加不加1都可，无非是多一位还是少一位，此处指follower对应index为空
      }
    } else {
      if (reply.conflict_term == -100) {
      }
      //-------------------很关键，运行时不能注释下面这段，因为我自己调试bug强行增加bug，没有专门的测试程序-----------------
      else
        raft->next_index_[clientPeerId] = reply.conflict_index;
    }
  }
  raft->SaveRaftState();
}

AppendEntriesReply Raft::AppendEntries(AppendEntriesArgs args) {
  std::vector<LogEntry> recvLog = GetCmdAndTerm(args.send_logs);
  AppendEntriesReply reply;
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
          peer_id_, LastIndex(), args.leader_id_, args.prev_log_index,
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
             args.leader_id_, args.prev_log_term);

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
    commit_index_ = min(args.leader_commit, LastIndex());
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
  dead_ = 1;
  printf("raft%d is dead_\n", peer_id_);
}

void Raft::Activate() {
  dead_ = 0;
  printf("raft%d is Activate\n", peer_id_);
}

StartRet Raft::Start(Operation op) {
  StartRet ret;
  std::unique_lock<std::mutex> lock(mutex_);
  RAFT_STATE state = state_;
  if (state != LEADER) {
    // printf("index : %d, term : %d, isleader : %d\n", ret.cmd_index,
    // ret.curr_term_, ret.is_leader == false ? 0 : 1);
    return ret;
  }

  LogEntry log;
  log.command_str = op.GetCmd();
  log.term = curr_term_;
  PushBackLog(log);

  ret.cmd_index = LastIndex();
  ret.curr_term_ = curr_term_;
  ret.is_leader = true;
  // printf("index : %d, term : %d, isleader : %d\n", ret.cmd_index,
  // ret.curr_term_, ret.is_leader == false ? 0 : 1);

  return ret;
}

void Raft::PrintLogs() {
  for (const auto& a : logs_) {
    printf("logs : %d\n", a.term);
  }
  cout << endl;
}

void Raft::Serialize() {
  std::string str;
  str += std::to_string(this->persister_.curr_term) + ";" +
         std::to_string(this->persister_.voted_for) + ";";
  str += std::to_string(this->persister_.last_included_index) + ";" +
         std::to_string(this->persister_.last_included_term) + ";";
  for (const auto& log : this->persister_.logs) {
    str += log.command_str + "," + std::to_string(log.term) + ".";
  }
  std::string filename = "persister_-" + std::to_string(peer_id_);
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    std::perror("open");
    std::exit(-1);
  }
  int len = ::write(fd, str.c_str(), str.size());
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
  int length = ::lseek(fd, 0, SEEK_END);
  ::lseek(fd, 0, SEEK_SET);
  char buf[length];
  ::bzero(buf, length);
  int len = ::read(fd, buf, length);
  if (len != length) {
    std::perror("read");
    std::exit(-1);
  }
  ::close(fd);
  std::string content(buf);
  std::vector<std::string> persist;
  std::string tmp = "";
  for (int i = 0; i < content.size(); i++) {
    if (content[i] != ';') {
      tmp += content[i];
    } else {
      if (tmp.size() != 0) persist.push_back(tmp);
      tmp = "";
    }
  }
  persist.push_back(tmp);
  this->persister_.curr_term = std::atoi(persist[0].c_str());
  this->persister_.voted_for = std::atoi(persist[1].c_str());
  this->persister_.last_included_index = std::atoi(persist[2].c_str());
  this->persister_.last_included_term = std::atoi(persist[3].c_str());
  std::vector<std::string> log;
  std::vector<LogEntry> logs;
  tmp = "";
  for (int i = 0; i < persist[4].size(); i++) {
    if (persist[4][i] != '.') {
      tmp += persist[4][i];
    } else {
      if (tmp.size() != 0) log.push_back(tmp);
      tmp = "";
    }
  }
  for (int i = 0; i < log.size(); i++) {
    tmp = "";
    int j = 0;
    for (; j < log[i].size(); j++) {
      if (log[i][j] != ',') {
        tmp += log[i][j];
      } else
        break;
    }
    std::string number(log[i].begin() + j + 1, log[i].end());
    int num = std::atoi(number.c_str());
    logs.push_back(LogEntry(tmp, num));
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
         curr_term_, voted_for_, logs_.size());
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
  for (int i = 0; i < persister_.logs.size(); i++) {
    sum += persister_.logs[i].command_str.size() + 3;
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
      peer_id_, logs_.size(), compressLen, last_included_index_);

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
  printf("[%d] after log.size is %d\n", peer_id_, logs_.size());
  //更新了logs及lastTerm和lastIndex，需要持久化
  persister_.snap_shot = snap_shot;
  SaveRaftState();

  SaveSnapShot();
  printf("[%d] persister_.size is %d, last_included_index is %d\n", peer_id_,
         persister_.logs.size(), last_included_index_);
}

int Raft::IdxToCompressLogPos(int index) {
  return index - this->last_included_index_ - 1;
}

bool Raft::ReadSnapShot() {
  std::string filename = "snap_shot-" + std::to_string(peer_id_);
  if (::access(filename.c_str(), F_OK) == -1) return false;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    std::perror("::open");
    return false;
  }
  int length = ::lseek(fd, 0, SEEK_END);
  ::lseek(fd, 0, SEEK_SET);
  char buf[length];
  ::bzero(buf, length);
  int len = ::read(fd, buf, length);
  if (len != length) {
    std::perror("::read");
    std::exit(-1);
  }
  ::close(fd);
  std::string snap_shot(buf);
  persister_.snap_shot = snap_shot;
  return true;
}

void Raft::SaveSnapShot() {
  std::string filename = "snap_shot-" + std::to_string(peer_id_);
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    std::perror("::open");
    std::exit(-1);
  }
  int len = ::write(fd, persister_.snap_shot.c_str(),
                    persister_.snap_shot.size() + 1);
  ::close(fd);
}

void Raft::InstallSnapShotTokvServer() {
  // while(1){
  //     m_lock.lock();
  //     installSnapShotFlag = true;
  //     printf("%d install to kvserver, install is %d but apply is %d\n",
  //         peer_id_, installSnapShotFlag ? 1 : 0, applyLogFlag ? 1 : 0);
  //     if(applyLogFlag){
  //         m_lock.unlock();
  //         ::usleep(1000);
  //         continue;
  //     }
  //     break;
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
  msg.snap_shot = persister_.snap_shot;
  msg.last_included_index = this->last_included_index_;
  msg.last_included_term = this->last_included_term_;

  last_applied_ = last_included_index_;
  lock.unlock();

  WaitRecvSem();
  msgs_.push_back(msg);
  PostSendSem();

  // m_lock.lock();
  // installSnapShotFlag = false;
  // applyLogFlag = true;
  // m_lock.unlock();
  printf("%d call install RPC\n", peer_id_);
}

int Raft::LastIndex() { return last_included_index_ + logs_.size(); }

int Raft::LastTerm() {
  int LastTerm = last_included_term_;
  if (logs_.size() != 0) {
    LastTerm = logs_.back().term;
  }
  return LastTerm;
}

#if 1
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