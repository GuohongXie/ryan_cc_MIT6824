#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "buttonrpc/buttonrpc.hpp"

constexpr int COMMOM_PORT = 1234;
constexpr int HEART_BEART_PERIOD = 100000;

//需要结合LAB3实现应用层dataBase和Raft交互用的，通过getCmd()转化为applyMsg的command
//实际上这只是LAB2的raft.hpp，在LAB3中改了很多，LAB4又改了不少，所以每个LAB都引了单独的raft.hpp
struct Operation {
  std::string GetCmd() const {
    std::string cmd = op + " " + key + " " + value;
    return cmd;
  }

  std::string op;
  std::string key;
  std::string value;
  int client_id{-1};
  int request_id{-1};
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

//一个存放当前raft的ID及自己两个RPC端口号的class(为了减轻负担，一个选举，一个日志同步，分开来)
struct PeersInfo {
  std::pair<int, int> port;
  int peer_id_{-1};
};

//日志
struct LogEntry {
  explicit LogEntry(std::string cmd = "", int term_tmp = -1)
      : command(cmd), term(term_tmp) {}
  std::string command;
  int term;
};

//持久化类，LAB2中需要持久化的内容就这3个，后续会修改
struct Persister {
  std::vector<LogEntry> logs;
  int curr_term_{-1};
  int voted_for_{-1};
};

struct AppendEntriesArgs {
  // AppendEntriesArgs():term(-1), leader_id_(-1), prev_log_index(-1),
  // prev_log_term(-1){
  //     //leader_commit = 0;
  //     send_logs.clear();
  // }
  int term{-1};
  int leader_id_{-1};
  int prev_log_index{-1};
  int prev_log_term{-1};
  int leader_commit{0};
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

struct AppendEntriesReply {
  int term{-1};
  bool is_successful{false};
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
  static void* ListenForVote(void* arg);    //用于监听voteRPC的server线程
  static void* ListenForAppend(void* arg);  //用于监听appendRPC的server线程
  static void* ProcessEntriesLoop(void* arg);  //持续处理日志同步的守护线程
  static void* ElectionLoop(void* arg);     //持续处理选举的守护线程
  static void* CallRequestVote(void* arg);  //发voteRPC的线程
  static void* SendAppendEntries(void* arg);  //发appendRPC的线程
  static void* ApplyLogLoop(void* arg);  //持续向上层应用日志的守护线程
  // static void* apply(void* arg);
  // static void* save(void* arg);
  enum RAFT_STATE {
    LEADER = 0,
    CANDIDATE,
    FOLLOWER
  };  //用枚举定义的raft三种状态
  void Make(std::vector<PeersInfo> peers, int id);  // raft初始化
  int GetMyduration(const timeval& last);  //传入某个特定计算到当下的持续时间
  void
  SetBroadcastTime();  //重新设定BroadcastTime，成为leader发心跳的时候需要重置
  std::pair<int, bool>
  GetState();  //在LAB3中会用到，提前留出来的接口判断是否leader
  RequestVoteReply RequestVote(RequestVoteArgs args);  // vote的RPChandler
  AppendEntriesReply AppendEntries(
      AppendEntriesArgs args);  // append的RPChandler
  bool CheckLogUptodate(int term,
                        int index);  //判断是否最新日志(两个准则)，vote时会用到
  void PushBackLog(LogEntry log);  //插入新日志
  std::vector<LogEntry> GetCmdAndTerm(
      std::string
          text);  //用的RPC不支持传容器，所以封装成string，这个是解封装恢复函数
  StartRet Start(
      Operation op);  //向raft传日志的函数，只有leader响应并立即返回，应用层用到
  void PrintLogs();

  void Serialize();      //序列化
  bool Deserialize();    //反序列化
  void SaveRaftState();  //持久化
  void ReadRaftState();  //读取持久化状态
  bool IsKilled();       //->check is killed?
  void Kill();  //设定raft状态为dead，LAB3B快照测试时会用到

 private:
  //成员变量不一一注释了，基本在论文里都有，函数实现也不注释了，看过论文看过我写的函数说明
  //自然就能理解了，不然要写太多了，这样整洁一点，注释了太乱了，论文才是最关键的
  std::mutex mutex_;
  std::condition_variable cond_;
  std::vector<PeersInfo> peers_;
  Persister persister_;
  int peer_id_;
  int is_dead_;

  //需要持久化的data
  int curr_term_;
  int voted_for_;
  std::vector<LogEntry> logs_;

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
};

void Raft::Make(std::vector<PeersInfo> peers, int id) {
  peers_ = peers;
  // this->persister_ = persister_;
  peer_id_ = id;
  is_dead_ = 0;

  state_ = FOLLOWER;
  curr_term_ = 0;
  leader_id_ = -1;
  voted_for_ = -1;
  // last_wake_time_ = std::chrono::system_clock::now();
  ::gettimeofday(&last_wake_time_, nullptr);
  // readPersist(persister_.ReadRaftState());

  // for(int i = 0; i < id + 1; i++){
  //     LogEntry log;
  //     log.command = to_string(i);
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

  ReadRaftState();

  // Using std::thread instead of pthread
  std::thread listen_thread1(&Raft::ListenForVote, this);
  listen_thread1.detach();
  std::thread listen_thread2(&Raft::ListenForAppend, this);
  listen_thread2.detach();
  std::thread listen_thread3(&Raft::ApplyLogLoop, this);
  listen_thread3.detach();
}

void* Raft::ApplyLogLoop(void* arg) {
  // Raft* raft = (Raft*)arg;
  Raft* raft = static_cast<Raft*>(arg);
  while (!raft->is_dead_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::lock_guard<std::mutex> lock(raft->mutex_);
    for (int i = raft->last_applied_; i < raft->commit_index_; i++) {
      //  @brief 封装好信息发回给客户端, LAB3中会用
      //  ApplyMsg msg;
    }
    raft->last_applied_ = raft->commit_index_;
  }
}

int Raft::GetMyduration(const timeval& last) {
  struct timeval now;
  ::gettimeofday(&now, nullptr);
  // printf("--------------------------------\n");
  // printf("now's sec : %ld, now's usec : %ld\n", now.tv_sec, now.tv_usec);
  // printf("last's sec : %ld, last's usec : %ld\n", last.tv_sec, last.tv_usec);
  // printf("%d\n", ((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec -
  // last.tv_usec))); printf("--------------------------------\n");
  return ((now.tv_sec - last.tv_sec) * 1000000 + (now.tv_usec - last.tv_usec));
}

//稍微解释下-200000us是因为让记录的m_lastBroadcastTime变早，这样在appendLoop中getMyduration(last_broadcast_time_)直接达到要求
//因为心跳周期是100000us
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
  // Raft* raft = (Raft*)arg;
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc server;
  server.as_server(raft->peers_[raft->peer_id_].port.first);
  server.bind("RequestVote", &Raft::RequestVote, raft);

  std::thread election_thread(&Raft::ElectionLoop, raft);
  election_thread.detach();

  server.run();
  printf("exit!\n");
}

void* Raft::ListenForAppend(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc server;
  server.as_server(raft->peers_[raft->peer_id_].port.second);
  server.bind("AppendEntries", &Raft::AppendEntries, raft);

  std::thread processEntriesThread(&Raft::ProcessEntriesLoop, raft);
  processEntriesThread.detach();

  server.run();
  printf("exit!\n");
}

void* Raft::ElectionLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  bool resetFlag = false;
  while (!raft->is_dead_) {
    int time_out = ::rand() % 200000 + 200000;
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      std::unique_lock<std::mutex> lock(raft->mutex_);

      int during_time = raft->GetMyduration(raft->last_wake_time_);
      if (raft->state_ == FOLLOWER && during_time > time_out) {
        raft->state_ = CANDIDATE;
      }

      if (raft->state_ == CANDIDATE && during_time > time_out) {
        printf(" %d attempt election at term %d, time_out is %d\n",
               raft->peer_id_, raft->curr_term_, time_out);
        ::gettimeofday(&raft->last_wake_time_, nullptr);
        resetFlag = true;
        raft->curr_term_++;
        raft->voted_for_ = raft->peer_id_;
        raft->SaveRaftState();

        raft->recv_votes_ = 1;
        raft->finished_vote_ = 1;
        raft->curr_peer_id_ = 0;

        std::vector<std::thread> threads;
        for (auto& server : raft->peers_) {
          if (server.peer_id_ == raft->peer_id_) continue;
          std::thread(CallRequestVote, raft).detach();
        }

        // for(auto& thread : threads) {
        //    if(thread.joinable()) {
        //        thread.detach();
        //    }
        //}

        while (raft->recv_votes_ <= raft->peers_.size() / 2 &&
               raft->finished_vote_ != raft->peers_.size()) {
          raft->cond_.wait(lock);
        }
        if (raft->state_ != CANDIDATE) {
          lock.unlock();
          continue;
        }
        if (raft->recv_votes_ > raft->peers_.size() / 2) {
          raft->state_ = LEADER;

          for (int i = 0; i < raft->peers_.size(); i++) {
            raft->next_index_[i] = raft->logs_.size() + 1;
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
  Raft* raft = (Raft*)arg;
  buttonrpc client;
  std::unique_lock<std::mutex> lock(raft->mutex_);
  RequestVoteArgs args;
  args.candidate_id = raft->peer_id_;
  args.term = raft->curr_term_;
  args.last_log_index = raft->logs_.size();
  args.last_log_term = !raft->logs_.empty() ? raft->logs_.back().term : 0;

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

void* Raft::ProcessEntriesLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  while (!raft->is_dead_) {
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
    // raft->curr_term_);
    lock.unlock();

    for (const auto& server : raft->peers_) {
      if (server.peer_id_ == raft->peer_id_) continue;
      std::thread(&Raft::SendAppendEntries, raft).detach();
    }
  }
}

std::vector<LogEntry> Raft::GetCmdAndTerm(std::string text) {
  std::vector<LogEntry> logs;
  int N = text.size();
  std::vector<std::string> str;
  std::string tmp = "";
  for (int i = 0; i < N; i++) {
    if (text[i] != ';') {
      tmp += text[i];
    } else {
      if (!tmp.empty()) str.push_back(tmp);
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
    logs.emplace_back(LogEntry(tmp, num));
  }
  return logs;
}

void Raft::PushBackLog(LogEntry log) { logs_.push_back(log); }

void* Raft::SendAppendEntries(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc client;
  AppendEntriesArgs args;
  std::unique_lock<std::mutex> lock(raft->mutex_);

  if (raft->curr_peer_id_ == raft->peer_id_) {
    raft->curr_peer_id_++;
  }
  int clientPeerId = raft->curr_peer_id_;
  client.as_client("127.0.0.1",
                   raft->peers_[raft->curr_peer_id_++].port.second);

  args.term = raft->curr_term_;
  args.leader_id_ = raft->peer_id_;
  args.prev_log_index = raft->next_index_[clientPeerId] - 1;
  args.leader_commit = raft->commit_index_;

  for (int i = args.prev_log_index; i < raft->logs_.size(); i++) {
    args.send_logs +=
        (raft->logs_[i].command + "," + std::to_string(raft->logs_[i].term) + ";");
  }
  if (args.prev_log_index == 0) {
    args.prev_log_term = 0;
    if (!raft->logs_.empty()) {
      args.prev_log_term = raft->logs_[0].term;
    }
  } else
    args.prev_log_term = raft->logs_[args.prev_log_index - 1].term;

  printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n", raft->peer_id_,
         clientPeerId, args.prev_log_index, args.prev_log_term);

  if (raft->curr_peer_id_ == raft->peers_.size() ||
      (raft->curr_peer_id_ == raft->peers_.size() - 1 &&
       raft->peer_id_ == raft->curr_peer_id_)) {
    raft->curr_peer_id_ = 0;
  }
  lock.unlock();
  AppendEntriesReply reply =
      client.call<AppendEntriesReply>("AppendEntries", args).val();

  lock.lock();
  if (reply.term > raft->curr_term_) {
    raft->state_ = FOLLOWER;
    raft->curr_term_ = reply.term;
    raft->voted_for_ = -1;
    raft->SaveRaftState();
    return nullptr;  // FOLLOWER没必要维护nextIndex,成为leader会更新
  }

  if (reply.is_successful) {
    raft->next_index_[clientPeerId] +=
        raft->GetCmdAndTerm(args.send_logs).size();
    raft->match_index_[clientPeerId] = raft->next_index_[clientPeerId] - 1;

    std::vector<int> tmpIndex = raft->match_index_;
    std::sort(tmpIndex.begin(), tmpIndex.end());
    int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
    if (realMajorityMatchIndex > raft->commit_index_ &&
        raft->logs_[realMajorityMatchIndex - 1].term == raft->curr_term_) {
      raft->commit_index_ = realMajorityMatchIndex;
    }
  }

  if (!reply.is_successful) {
    // if(!raft->m_firstIndexOfEachTerm.count(reply.conflict_term)){
    //     raft->next_index_[clientPeerId]--;
    // }else{
    //     raft->next_index_[clientPeerId] = min(reply.conflict_index,
    //     raft->m_firstIndexOfEachTerm[reply.conflict_term]);
    // }

    if (reply.conflict_term != -1) {
      int leader_conflict_index = -1;
      for (int index = args.prev_log_index; index >= 1; index--) {
        if (raft->logs_[index - 1].term == reply.conflict_term) {
          leader_conflict_index = index;
          break;
        }
      }
      if (leader_conflict_index != -1) {
        raft->next_index_[clientPeerId] = leader_conflict_index + 1;
      } else {
        raft->next_index_[clientPeerId] = reply.conflict_index;
      }
    } else {
      raft->next_index_[clientPeerId] = reply.conflict_index + 1;
    }
  }
  raft->SaveRaftState();
}

AppendEntriesReply Raft::AppendEntries(AppendEntriesArgs args) {
  std::vector<LogEntry> recvLog = GetCmdAndTerm(args.send_logs);
  AppendEntriesReply reply;
  std::unique_lock<std::mutex> lock(mutex_);
  reply.term = curr_term_;
  reply.is_successful = false;
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
      peer_id_, args.leader_id_, curr_term_, args.term,
      GetMyduration(last_wake_time_));
  ::gettimeofday(&last_wake_time_, nullptr);
  // persister_()

  int logSize = 0;
  if (logs_.empty()) {
    for (const auto& log : recvLog) {
      PushBackLog(log);
    }
    SaveRaftState();
    logSize = logs_.size();
    if (commit_index_ < args.leader_commit) {
      commit_index_ = std::min(args.leader_commit, logSize);
    }
    lock.unlock();
    reply.is_successful = true;
    // SaveRaftState();
    return reply;
  }

  if (logs_.size() < args.prev_log_index) {
    printf(" [%d]'s logs.size : %d < [%d]'s prevLogIdx : %d\n", peer_id_,
           logs_.size(), args.leader_id_, args.prev_log_index);
    reply.conflict_index = logs_.size();  //索引要加1
    lock.unlock();
    reply.is_successful = false;
    return reply;
  }
  if (args.prev_log_index > 0 &&
      logs_[args.prev_log_index - 1].term != args.prev_log_term) {
    printf(" [%d]'s prevLogterm : %d != [%d]'s prevLogTerm : %d\n", peer_id_,
           logs_[args.prev_log_index - 1].term, args.leader_id_,
           args.prev_log_term);

    reply.conflict_term = logs_[args.prev_log_index - 1].term;
    for (int index = 1; index <= args.prev_log_index; index++) {
      if (logs_[index - 1].term == reply.conflict_term) {
        reply.conflict_index = index;  //找到冲突term的第一个index,比索引要加1
        break;
      }
    }
    lock.unlock();
    reply.is_successful = false;
    return reply;
  }

  logSize = logs_.size();
  for (int i = args.prev_log_index; i < logSize; i++) {
    logs_.pop_back();
  }
  // logs_.insert(logs_.end(), recvLog.begin(), recvLog.end());
  for (const auto& log : recvLog) {
    PushBackLog(log);
  }
  SaveRaftState();
  logSize = logs_.size();
  if (commit_index_ < args.leader_commit) {
    commit_index_ = std::min(args.leader_commit, logSize);
  }
  for (auto a : logs_) printf("%d ", a.term);
  printf(" [%d] sync success\n", peer_id_);
  lock.unlock();
  reply.is_successful = true;
  return reply;
}

std::pair<int, bool> Raft::GetState() {
  std::pair<int, bool> serverState;
  serverState.first = curr_term_;
  serverState.second = (state_ == LEADER);
  return serverState;
}

void Raft::Kill() { is_dead_ = 1; }

StartRet Raft::Start(Operation op) {
  StartRet ret;
  std::unique_lock<std::mutex> lock(mutex_);
  RAFT_STATE state = state_;
  if (state != LEADER) {
    return ret;
  }
  ret.cmd_index = logs_.size();
  ret.curr_term_ = curr_term_;
  ret.is_leader = true;

  LogEntry log;
  log.command = op.GetCmd();
  log.term = curr_term_;
  PushBackLog(log);

  return ret;
}

void Raft::PrintLogs() {
  for (auto a : logs_) {
    printf("logs : %d\n", a.term);
  }
  std::cout << std::endl;
}

void Raft::Serialize() {
  std::string str;
  str += std::to_string(this->persister_.curr_term_) + ";" +
         std::to_string(this->persister_.voted_for_) + ";";
  for (const auto& log : this->persister_.logs) {
    str += log.command + "," + std::to_string(log.term) + ".";
  }
  std::string filename = "persister_-" + std::to_string(peer_id_);
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    std::perror("open");
    std::exit(-1);
  }
  int len = ::write(fd, str.c_str(), str.size());
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
    perror("read");
    exit(-1);
  }
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
  this->persister_.curr_term_ = atoi(persist[0].c_str());
  this->persister_.voted_for_ = atoi(persist[1].c_str());
  std::vector<std::string> log;
  std::vector<LogEntry> logs;
  tmp = "";
  for (int i = 0; i < persist[2].size(); i++) {
    if (persist[2][i] != '.') {
      tmp += persist[2][i];
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
    logs.emplace_back(tmp, num);
  }
  this->persister_.logs = logs;
  return true;
}

void Raft::ReadRaftState() {
  //只在初始化的时候调用，没必要加锁，因为run()在其之后才执行
  bool ret = this->Deserialize();
  if (!ret) return;
  this->curr_term_ = this->persister_.curr_term_;
  this->voted_for_ = this->persister_.voted_for_;

  for (const auto& log : this->persister_.logs) {
    PushBackLog(log);
  }
  printf(" [%d]'s term : %d, votefor : %d, logs.size() : %d\n", peer_id_,
         curr_term_, voted_for_, logs_.size());
}

void Raft::SaveRaftState() {
  persister_.curr_term_ = curr_term_;
  persister_.voted_for_ = voted_for_;
  persister_.logs = logs_;
  Serialize();
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "loss parameter of peers_num" << std::endl;
    return -1;
  }
  int peers_num = std::stoi(argv[1]);
  if (peers_num % 2 == 0) {
    std::cout << "the peers_num should be odd" << std::endl;
    return -1;
  }

  std::srand((unsigned)std::time(nullptr));
  std::vector<PeersInfo> peers(peers_num);
  for (int i = 0; i < peers_num; i++) {
    peers[i].peer_id_ = i;
    peers[i].port.first = COMMOM_PORT + i;                  // vote的RPC端口
    peers[i].port.second = COMMOM_PORT + i + peers.size();  // append的RPC端口
    // printf(" id : %d port1 : %d, port2 : %d\n", peers[i].peer_id_,
    // peers[i].port.first, peers[i].port.second);
  }

  std::vector<std::unique_ptr<Raft>> rafts;
  rafts.reserve(peers_num);
  // Raft* rafts = new Raft[peers.size()];
  for (int i = 0; i < peers_num; ++i) {
    rafts.push_back(std::make_unique<Raft>());
    rafts[i]->Make(peers, i);
  }
  // for (int i = 0; i < peers.size(); i++) {
  //  rafts[i].Make(peers, i);
  //}

  //------------------------------test部分--------------------------
  ::usleep(400000);
  for (int i = 0; i < peers_num; i++) {
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
      rafts[i]
          ->Kill();  // kill后选举及心跳的线程会宕机，会产生新的leader，很久之后了，因为上面传了1000条日志
      break;
    }
  }
  //------------------------------test部分--------------------------
  while (1)
    ;
}