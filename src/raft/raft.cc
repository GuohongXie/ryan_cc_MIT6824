#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "buttonrpc/buttonrpc.hpp"

constexpr int COMMOM_PORT = 1234;
constexpr int HEART_BEART_PERIOD = 100000;

//需要结合LAB3实现应用层dataBase和Raft交互用的，通过getCmd()转化为applyMsg的command
//实际上这只是LAB2的raft.hpp，在LAB3中改了很多，LAB4又改了不少，所以每个LAB都引了单独的raft.hpp


struct Operation {
  std::string op;
  std::string key;
  std::string value;
  int client_id{-1};
  int request_id{-1};
  std::string GetCmd() const {
    std::string cmd = op + " " + key + " " + value;
    return cmd;
  }
};

//通过传入raft.Start()得到的返回值，封装成类
struct StartRet {
  int cmd_index{-1};
  int curr_term_{-1};
  bool is_leader{false};
};

//同应用层交互的需要提交到应用层并apply的封装成applyMsg的日志信息
struct ApplyMsg {
  bool is_command_valid{false};
  std::string command;
  int command_index{-1};
};

//为了减轻负担，一个选举，一个日志同步，分开来
struct PeersInfo {
  std::pair<int, int> port;  //选举端口和日志同步端口
  int peer_id{-1}; //当前raft节点的ID
};

//日志
struct LogEntry {
  //这个构造函数不可少
  explicit LogEntry(std::string cmd = "", int term_tmp = -1)
      : command(std::move(cmd)), term(term_tmp) {}
  std::string command;
  int term{-1};
};

//2C
//持久化类，LAB2中需要持久化的内容就这3个，后续会修改
struct Persister {
  std::vector<LogEntry> logs;
  int curr_term{-1};
  int voted_for{-1};
};

struct AppendEntriesArgs {
  int term{-1};
  int leader_id{-1};
  int prev_log_index{-1};
  int prev_log_term{-1};
  int leader_commit{0};

  //论文中这里应该是一个日志的数组，
  //但是我自己实现的RPC不支持传容器，所以封装成string
  std::string entry;

  // 用于buttonrpc的序列化和反序列化
  friend Serializer& operator>>(Serializer& in, AppendEntriesArgs& d) {
    in >> d.term >> d.leader_id >> d.prev_log_index >> d.prev_log_term >>
        d.leader_commit >> d.entry;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, const AppendEntriesArgs& d) {
    out << d.term << d.leader_id << d.prev_log_index << d.prev_log_term
        << d.leader_commit << d.entry;
    return out;
  }
};

struct AppendEntriesReply {
  int term{-1};
  bool success{false};
  int conflict_term{-1};   //用于冲突时日志快速匹配
  int conflict_index{-1};  //用于冲突时日志快速匹配
};

struct RequestVoteArgs {
  int term{-1};
  int candidate_id{-1};
  int last_log_term{-1};
  int last_log_index{-1};
};

struct RequestVoteReply {
  int term{-1};
  bool vote_granted{false};
};

class Raft {
 public:

  // raft初始化
  //lab2中的要求，peers是所有raft节点的信息，id是当前raft节点的ID
  void Make(const std::vector<PeersInfo>& peers, int id);  

  enum RAFT_STATE {
    LEADER = 0,
    CANDIDATE,
    FOLLOWER
  };  //用枚举定义的raft三种状态

  void ListenForVote();    //用于监听voteRPC的server线程
  void ListenForAppend();  //用于监听appendRPC的server线程
  void ProcessEntriesLoop();  //持续处理日志同步的守护线程
  void ElectionLoop();     //持续处理选举的守护线程
  void CallRequestVote();  //发voteRPC的线程
  void SendAppendEntries();  //发appendRPC的线程
  void ApplyLogLoop();  //持续向上层应用日志的守护线程
  // void Apply();
  // void Save();

  //重新设定BroadcastTime，成为leader发心跳的时候需要重置
  void SetBroadcastTime(); 

  //在LAB3中会用到，提前留出来的接口判断是否leader
  std::pair<int, bool> GetState();  

  RequestVoteReply RequestVote(RequestVoteArgs args);  //vote的RPChandler
  AppendEntriesReply AppendEntries(const AppendEntriesArgs& args); //append的RPChandler

  bool CheckLogUptodate(int term, int index);  //判断是否最新日志(两个准则)，vote时会用到
  void PushBackLog(const LogEntry& log);  //插入新日志
  static std::vector<LogEntry> GetCmdAndTerm(std::string text);  //用的RPC不支持传容器，所以封装成string，这个是解封装恢复函数
  StartRet Start(const Operation& op);  //向raft传日志的函数，只有leader响应并立即返回，应用层用到

  void PrintLogs();

  void Serialize();      //序列化
  bool Deserialize();    //反序列化
  void SaveRaftState();  //持久化, 2C
  void ReadRaftState();  //读取持久化状态, 2C
  bool IsKilled();       //->check is killed?
  void Kill();  //设定raft状态为dead，LAB3B快照测试时会用到

 private:
  static int GetMyduration(const timeval& last);  //传入某个特定计算到当下的持续时间

  //成员变量见raft论文
  std::mutex mutex_;
  std::condition_variable cond_;
  std::vector<PeersInfo> peers_;
  Persister persister_;
  int peer_id_;
  int is_dead_;

  //需要持久化的state
  int curr_term_; //服务器已知的最新任期（初始化为 0，持续递增）
  int voted_for_; //在当前获得选票的候选人的 Id
  std::vector<LogEntry> logs_;

  //所有服务器上的易失性状态
  int last_applied_; //已知的最大的已经被提交的日志条目的索引值
  int commit_index_;

  //leader上的易失性状态
  std::vector<int> next_index_;
  std::vector<int> match_index_;


  int recv_votes_;
  int finished_vote_;
  int curr_peer_id_;

  RAFT_STATE state_;

  int leader_id_;
  struct timeval last_wake_time_;
  struct timeval last_broadcast_time_;
};

void Raft::Make(const std::vector<PeersInfo>& peers, int id) {
  peers_ = peers;
  peer_id_ = id;
  is_dead_ = false;

  state_ = FOLLOWER;
  curr_term_ = 0;
  leader_id_ = -1;
  voted_for_ = -1;
  ::gettimeofday(&last_wake_time_, nullptr);

  recv_votes_ = 0;
  finished_vote_ = 0;
  curr_peer_id_ = 0;

  last_applied_ = 0; //初始化为0， 单调递增
  commit_index_ = 0;
  next_index_.resize(peers.size(), 1);
  match_index_.resize(peers.size(), 0);

  ReadRaftState();

  std::thread listen_thread1(&Raft::ListenForVote, this);
  listen_thread1.detach();
  std::thread listen_thread2(&Raft::ListenForAppend, this);
  listen_thread2.detach();
  std::thread listen_thread3(&Raft::ApplyLogLoop, this);
  listen_thread3.detach();
}

void Raft::ApplyLogLoop() {
  while (!is_dead_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::lock_guard<std::mutex> lock(mutex_);
    for (int i = last_applied_; i < commit_index_; i++) {
      //  @brief 封装好信息发回给客户端, LAB3中会用
      //  ApplyMsg msg;
    }
    last_applied_ = commit_index_;
  }
}

/// @brief 计算传入的时间到当前时间的时间间隔(微秒)
int Raft::GetMyduration(const timeval& last) {
  struct timeval now{};
  ::gettimeofday(&now, nullptr);
  return (static_cast<int>((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec - last.tv_usec)));
}

//稍微解释下-200000us是因为让记录的last_broadcast_time_变早，
//这样在AppendLoop中GetMyduration(last_broadcast_time_)直接达到要求
//因为心跳周期是100,000 us
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

  std::thread election_thread(&Raft::ElectionLoop, this);
  election_thread.detach();

  server.run();
  printf("exit!\n");
}

void Raft::ListenForAppend() {
  buttonrpc server;
  server.as_server(peers_[peer_id_].port.second);
  server.bind("AppendEntries", &Raft::AppendEntries, this);

  std::thread processEntriesThread(&Raft::ProcessEntriesLoop, this);
  processEntriesThread.detach();

  server.run();
  printf("exit!\n");
}

void Raft::ElectionLoop() {
  bool resetFlag = false;
  while (!is_dead_) {
    int time_out = ::rand() % 200000 + 200000; //随机化选举超时时间
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      std::unique_lock<std::mutex> lock(mutex_);

      int during_time = GetMyduration(last_wake_time_);
      if (state_ == FOLLOWER && during_time > time_out) {
        state_ = CANDIDATE;
      }

      if (state_ == CANDIDATE && during_time > time_out) {
        printf(" %d attempt election at term %d, time_out is %d\n",
               peer_id_, curr_term_, time_out);
        ::gettimeofday(&last_wake_time_, nullptr);
        resetFlag = true;
        curr_term_++;
        voted_for_ = peer_id_;
        SaveRaftState();

        recv_votes_ = 1;
        finished_vote_ = 1;
        curr_peer_id_ = 0;

        std::vector<std::thread> threads;
        for (auto& server : peers_) {
          if (server.peer_id == peer_id_) continue;
          std::thread(&Raft::CallRequestVote, this).detach();
        }

        while (recv_votes_ <= peers_.size() / 2 &&
               finished_vote_ != peers_.size()) {
          cond_.wait(lock);
        }
        if (state_ != CANDIDATE) {
          lock.unlock();
          continue;
        }
        if (recv_votes_ > peers_.size() / 2) {
          state_ = LEADER;

          for (int i = 0; i < peers_.size(); i++) {
            next_index_[i] = static_cast<int>(logs_.size() + 1);
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
  RequestVoteArgs args;
  args.candidate_id = peer_id_;
  args.term = curr_term_;
  args.last_log_index = static_cast<int>(logs_.size());
  args.last_log_term = !logs_.empty() ? logs_.back().term : 0;

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
  lock.unlock();
}

bool Raft::CheckLogUptodate(int term, int index) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (logs_.empty()) {
    return true;
  }
  if (term > logs_.back().term) {
    return true;
  }
  if (term == logs_.back().term && index >= logs_.size()) {
    return true;
  }
  return false;
}

/// @brief Follower和Candidate在收到RPC时，都会调用这个函数
RequestVoteReply Raft::RequestVote(RequestVoteArgs args) {
  RequestVoteReply reply;
  reply.vote_granted = false;
  std::unique_lock<std::mutex> lock(mutex_);
  //但凡涉及到对共享数据的读写，都需要加锁
  reply.term = curr_term_;

  if (curr_term_ > args.term) {
    return reply;
  }

  if (curr_term_ < args.term) { // TODO: <= ?
    state_ = FOLLOWER;
    curr_term_ = args.term;
    voted_for_ = -1;
  }

  if (voted_for_ == -1 || voted_for_ == args.candidate_id) {
    lock.unlock();
    bool ret = CheckLogUptodate(args.last_log_term, args.last_log_index);
    if (!ret) return reply;

    lock.lock();
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
  //Raft* raft = static_cast<Raft*>(arg);
  while (!is_dead_) {
    ::usleep(1000);
    std::unique_lock<std::mutex> lock(mutex_);
    if (state_ != LEADER) {
      continue;
    }

    // printf("sec : %ld, usec : %ld\n", last_broadcast_time_.tv_sec,
    // last_broadcast_time_.tv_usec);
    int during_time = this->GetMyduration(this->last_broadcast_time_);
    // printf("time is %d\n", during_time);
    if (during_time < HEART_BEART_PERIOD) {
      continue;
    }

    ::gettimeofday(&this->last_broadcast_time_, nullptr);
    // printf("%d send AppendRetries at %d\n", peer_id_,
    // curr_term_);
    lock.unlock();

    for (const auto& server : this->peers_) {
      if (server.peer_id == this->peer_id_) continue;
      std::thread(&Raft::SendAppendEntries, this).detach();
    }
  }
}

std::vector<LogEntry> Raft::GetCmdAndTerm(std::string text) {
  std::vector<LogEntry> logs;
  size_t N = text.size();
  std::vector<std::string> logs_str;
  std::string tmp;
  for (size_t i = 0; i < N; i++) {
    if (text[i] != ';') {
      tmp += text[i];
    } else {
      if (!tmp.empty()) logs_str.push_back(tmp);
      tmp = "";
    }
  }
  for (auto& i : logs_str) {
    tmp = "";
    int j = 0;
    for (; j < i.size(); j++) {
      if (i[j] != ',') {
        tmp += i[j];
      } else
        break;
    }
    std::string number(i.begin() + j + 1, i.end());
    int num = std::stoi(number);
    logs.emplace_back(LogEntry(tmp, num));
  }
  return logs;
}

void Raft::PushBackLog(const LogEntry& log) { logs_.push_back(log); }

void Raft::SendAppendEntries() {
  buttonrpc client;
  AppendEntriesArgs args;
  std::unique_lock<std::mutex> lock(mutex_);

  if (curr_peer_id_ == peer_id_) {
    curr_peer_id_++;
  }
  int clientPeerId = curr_peer_id_;
  client.as_client("127.0.0.1",
                   peers_[curr_peer_id_++].port.second);

  args.term = curr_term_;
  args.leader_id = peer_id_;
  args.prev_log_index = next_index_[clientPeerId] - 1;
  args.leader_commit = commit_index_;

  for (int i = args.prev_log_index; i < logs_.size(); i++) {
    args.entry += (logs_[i].command + "," +
                       std::to_string(logs_[i].term) + ";");
  }
  if (args.prev_log_index == 0) {
    args.prev_log_term = 0;
    if (!logs_.empty()) {
      args.prev_log_term = logs_[0].term;
    }
  } else
    args.prev_log_term = logs_[args.prev_log_index - 1].term;

  printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n", peer_id_,
         clientPeerId, args.prev_log_index, args.prev_log_term);

  if (curr_peer_id_ == peers_.size() ||
      (curr_peer_id_ == peers_.size() - 1 &&
       peer_id_ == curr_peer_id_)) {
    curr_peer_id_ = 0;
  }
  lock.unlock();
  AppendEntriesReply reply =
      client.call<AppendEntriesReply>("AppendEntries", args).val();

  lock.lock();
  if (reply.term > curr_term_) {
    state_ = FOLLOWER;
    curr_term_ = reply.term;
    voted_for_ = -1;
    SaveRaftState();
    return;  // FOLLOWER没必要维护nextIndex,成为leader会更新
  }

  if (reply.success) {
    next_index_[clientPeerId] += static_cast<int>(GetCmdAndTerm(args.entry).size());
    match_index_[clientPeerId] = next_index_[clientPeerId] - 1;

    std::vector<int> tmpIndex = match_index_;
    std::sort(tmpIndex.begin(), tmpIndex.end());
    int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
    if (realMajorityMatchIndex > commit_index_ &&
        logs_[realMajorityMatchIndex - 1].term == curr_term_) {
      commit_index_ = realMajorityMatchIndex;
    }
  }

  if (!reply.success) {
    if (reply.conflict_term != -1) {
      int leader_conflict_index = -1;
      for (int index = args.prev_log_index; index >= 1; index--) {
        if (logs_[index - 1].term == reply.conflict_term) {
          leader_conflict_index = index;
          break;
        }
      }
      if (leader_conflict_index != -1) {
        next_index_[clientPeerId] = leader_conflict_index + 1;
      } else {
        next_index_[clientPeerId] = reply.conflict_index;
      }
    } else {
      next_index_[clientPeerId] = reply.conflict_index + 1;
    }
  }
  SaveRaftState();
}

AppendEntriesReply Raft::AppendEntries(const AppendEntriesArgs& args) {
  std::vector<LogEntry> recvLog = GetCmdAndTerm(args.entry);
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
  printf(
      "[%d] recv append from [%d] at self term%d, send term %d, duration is "
      "%d\n",
      peer_id_, args.leader_id, curr_term_, args.term,
      GetMyduration(last_wake_time_));
  ::gettimeofday(&last_wake_time_, nullptr);
  // persister_()

  int log_size = 0;
  if (logs_.empty()) {
    for (const auto& log : recvLog) {
      PushBackLog(log);
    }
    SaveRaftState();
    log_size = static_cast<int>(logs_.size());
    if (commit_index_ < args.leader_commit) {
      commit_index_ = std::min(args.leader_commit, log_size);
    }
    lock.unlock();
    reply.success = true;
    // SaveRaftState();
    return reply;
  }

  if (logs_.size() < args.prev_log_index) {
    printf(" [%d]'s logs.size : %d < [%d]'s prevLogIdx : %d\n", peer_id_,
           static_cast<int>(logs_.size()), args.leader_id, args.prev_log_index);
    reply.conflict_index = static_cast<int>(logs_.size());  //索引要加1
    lock.unlock();
    reply.success = false;
    return reply;
  }
  if (args.prev_log_index > 0 &&
      logs_[args.prev_log_index - 1].term != args.prev_log_term) {
    printf(" [%d]'s prevLogterm : %d != [%d]'s prevLogTerm : %d\n", peer_id_,
           logs_[args.prev_log_index - 1].term, args.leader_id,
           args.prev_log_term);

    reply.conflict_term = logs_[args.prev_log_index - 1].term;
    for (int index = 1; index <= args.prev_log_index; index++) {
      if (logs_[index - 1].term == reply.conflict_term) {
        reply.conflict_index = index;  //找到冲突term的第一个index,比索引要加1
        break;
      }
    }
    lock.unlock();
    reply.success = false;
    return reply;
  }

  log_size = static_cast<int>(logs_.size());
  for (int i = args.prev_log_index; i < log_size; i++) {
    logs_.pop_back();
  }
  // logs_.insert(logs_.end(), recvLog.begin(), recvLog.end());
  for (const auto& log : recvLog) {
    PushBackLog(log);
  }
  SaveRaftState();
  log_size = static_cast<int>(logs_.size());
  if (commit_index_ < args.leader_commit) {
    commit_index_ = std::min(args.leader_commit, log_size);
  }
  for (const auto& a : logs_) printf("%d ", a.term);
  printf(" [%d] sync success\n", peer_id_);
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

void Raft::Kill() { is_dead_ = true; }

StartRet Raft::Start(const Operation& op) {
  StartRet ret;
  std::unique_lock<std::mutex> lock(mutex_);
  RAFT_STATE state = state_;
  if (state != LEADER) {
    return ret;
  }
  ret.cmd_index = static_cast<int>(logs_.size());
  ret.curr_term_ = curr_term_;
  ret.is_leader = true;

  LogEntry log;
  log.command = op.GetCmd();
  log.term = curr_term_;
  PushBackLog(log);

  return ret;
}

void Raft::PrintLogs() {
  for (const auto& a : logs_) {
    printf("logs : %d\n", a.term);
  }
  std::cout << std::endl;
}

/// @brief 在SaveRaftState()中调用，用于序列化当前raft节点的状态
/// 并将当前raft节点的状态保存到磁盘中
/// 保存的内容包括：persister_.curr_term, persister_.voted_for, persister_.logs
void Raft::Serialize() {
  std::string str;
  str += std::to_string(this->persister_.curr_term) + ";" +
         std::to_string(this->persister_.voted_for) + ";\n";
  for (const auto& log : this->persister_.logs) {
    // "\n"作为日志的分隔符
    str += log.command + ", " + std::to_string(log.term) + "\n";
  }
  std::string filename = "persister_-" + std::to_string(peer_id_);
  //以只写方式打开文件(不能从文件读数据)，若文件不存在则创建，
  //文件权限为0664, 0表示八进制, 0664 = 110 110 100
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    std::perror("open");
    std::exit(-1);
  }
  ssize_t len = ::write(fd, str.c_str(), str.size());
  if (len == -1) {
    std::perror("write");
    ::close(fd);  // 无论如何都需要关闭文件，所以这里也需要关闭。
    std::exit(-1);
  }
  ::close(fd);  // 正常关闭文件
}

/// @brief 从磁盘中读取保存的状态
bool Raft::Deserialize() {
  std::string filename = "persister_-" + std::to_string(peer_id_);

  // 从磁盘中读取保存的状态
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
    perror("read");
    ::close(fd);  //无论如何都需要关闭文件，所以这里也需要关闭
    exit(-1);
  }
  ::close(fd);  //正常关闭文件

  //将读取到的数据反序列化, 解析出来的数据保存到persister_中
  std::string content(buf);
  std::vector<std::string> persist;
  std::string tmp{};
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
  std::vector<std::string> logs_str;
  std::vector<LogEntry> logs;
  tmp = "";
  for (int i = 1; i < persist[2].size(); i++) {
    if (persist[2][i] != '\n') {
      tmp += persist[2][i];
    } else {
      if (!tmp.empty()) logs_str.push_back(tmp);
      tmp = "";
    }
  }
  for (auto & i : logs_str) {
    tmp = "";
    int j = 0;
    for (; j < i.size(); j++) {
      if (i[j] != ',') {
        tmp += i[j];
      } else
        break;
    }
    std::string number(i.begin() + j + 1, i.end());
    int num = std::stoi(number);
    logs.emplace_back(tmp, num);
  }
  this->persister_.logs = logs;
  return true;
}

/// @brief 读取持久化状态
void Raft::ReadRaftState() {
  //只在初始化的时候调用，没必要加锁，因为run()在其之后才执行
  bool ret = this->Deserialize();
  // TODO: 读取持久化状态失败的错误处理
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
  this->Serialize();
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "loss parameter of peers_num" << std::endl;
    return -1;
  }
  int peers_num = std::stoi(argv[1]);
  //简化raft的实现，只允许奇数个节点
  if (peers_num % 2 == 0) {
    std::cout << "the peers_num should be odd" << std::endl;
    return -1;
  }

  std::srand((unsigned)std::time(nullptr));
  std::vector<PeersInfo> peers(peers_num);
  for (int i = 0; i < peers_num; i++) {
    peers[i].peer_id = i;
    peers[i].port.first = COMMOM_PORT + i;                  // vote的RPC端口
    peers[i].port.second = COMMOM_PORT + i + static_cast<int>(peers.size());  // append的RPC端口
  }

  std::vector<std::unique_ptr<Raft>> rafts;
  rafts.reserve(peers_num);
  for (int i = 0; i < peers_num; ++i) {
    rafts.push_back(std::make_unique<Raft>());
    rafts[i]->Make(peers, i);
  }

  //------------------------------test部分--------------------------
  ::usleep(400000);
  for (int i = 0; i < peers_num; i++) {
    if (rafts[i]->GetState().second) { //如果不是主节点
      for (int j = 0; j < 100; j++) {
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
      // kill后选举及心跳的线程会宕机，会产生新的leader，
      // 很久之后了，因为上面传了1000条日志
      rafts[i]->Kill();  
      break;
    }
  }
  //------------------------------test部分--------------------------
  while (true) ;
}