#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "buttonrpc.hpp"
#include "locker.h"
using namespace std;

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
class InstallSnapShotArgs {
 public:
  int term;
  int leaderId;
  int lastIncludedIndex;
  int lastIncludedTerm;
  std::string snapShot;

  friend Serializer& operator>>(Serializer& in, InstallSnapShotArgs& d) {
    in >> d.term >> d.leaderId >> d.lastIncludedIndex >> d.lastIncludedTerm >>
        d.snapShot;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, InstallSnapShotArgs d) {
    out << d.term << d.leaderId << d.lastIncludedIndex << d.lastIncludedTerm
        << d.snapShot;
    return out;
  }
};

class InstallSnapSHotReply {
 public:
  int term;
};

class Operation {
 public:
  std::string getCmd();
  std::string op;
  std::string key;
  std::string value;
  int clientId;
  int requestId;
  int term;
  int index;
};

std::string Operation::getCmd() {
  std::string cmd = op + " " + key + " " + value + " " + to_string(clientId) + " " +
               to_string(requestId);
  return cmd;
}

class StartRet {
 public:
  StartRet() : m_cmdIndex(-1), curr_term_(-1), isLeader(false) {}
  int m_cmdIndex;
  int curr_term_;
  bool isLeader;
};

class ApplyMsg {
 public:
  bool commandValid;
  std::string command;
  int commandIndex;
  int commandTerm;
  Operation getOperation();

  int lastIncludedIndex;
  int lastIncludedTerm;
  std::string snapShot;
};

Operation ApplyMsg::getOperation() {
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
  operation.clientId = atoi(str[3].c_str());
  operation.requestId = atoi(str[4].c_str());
  operation.term = commandTerm;
  return operation;
}

class PeersInfo {
 public:
  std::pair<int, int> m_port;
  int peer_id_;
  bool isInstallFlag;
};

class LogEntry {
 public:
  LogEntry(std::string cmd = "", int term = -1) : m_command(cmd), m_term(term) {}
  std::string m_command;
  int m_term;
};

class Persister {
 public:
  std::vector<LogEntry> logs;
  std::string snapShot;
  int cur_term;
  int votedFor;
  int lastIncludedIndex;
  int lastIncludedTerm;
};

class AppendEntriesArgs {
 public:
  // AppendEntriesArgs():m_term(-1), leader_id_(-1), m_prevLogIndex(-1),
  // m_prevLogTerm(-1){
  //     //m_leaderCommit = 0;
  //     m_sendLogs.clear();
  // }
  int m_term;
  int leader_id_;
  int m_prevLogIndex;
  int m_prevLogTerm;
  int m_leaderCommit;
  std::string m_sendLogs;
  friend Serializer& operator>>(Serializer& in, AppendEntriesArgs& d) {
    in >> d.m_term >> d.leader_id_ >> d.m_prevLogIndex >> d.m_prevLogTerm >>
        d.m_leaderCommit >> d.m_sendLogs;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, AppendEntriesArgs d) {
    out << d.m_term << d.leader_id_ << d.m_prevLogIndex << d.m_prevLogTerm
        << d.m_leaderCommit << d.m_sendLogs;
    return out;
  }
};

class AppendEntriesReply {
 public:
  int m_term;
  bool m_success;
  int m_conflict_term;
  int m_conflict_index;
};

class RequestVoteArgs {
 public:
  int term;
  int candidateId;
  int lastLogTerm;
  int lastLogIndex;
};

class RequestVoteReply {
 public:
  int term;
  bool VoteGranted;
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
      std::string snapShot,
      int lastIncludedIndex);  //接受来自kvServer层的快照，用于持久化
  int IdxToCompressLogPos(int index);  //获得原先索引在截断日志后的索引
  bool ReadSnapShot();                 //读取快照
  void SaveSnapShot();                 //持久化快照
  void InstallSnapShotTokvServer();  //落后的raft向对应的应用层安装快照
  int LastIndex();                   //截断日志后的lastIndex
  int LastTerm();                    //截断日志后的lastTerm

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  locker m_lock;
  cond m_cond;
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
  sem m_recvSem;  //结合kvServer层的有名管道fifo模拟go的select及channel
  sem m_sendSem;  //结合kvServer层的有名管道fifo模拟go的select及channel
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
  //     log.m_command = to_string(i);
  //     log.m_term = i;
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
  //std::thread listen_thread1(&Raft::ListenForVote, this);
  //listen_thread1.detach();
  //std::thread listen_thread2(&Raft::ListenForAppend, this);
  //listen_thread2.detach();
  //std::thread listen_thread3(&Raft::ApplyLogLoop, this);
  //listen_thread3.detach();

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
        msg.command = raft->logs_[appliedIdx].m_command;
        msg.commandValid = true;
        msg.commandTerm = raft->logs_[appliedIdx].m_term;
        msg.commandIndex = raft->last_applied_;
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
  server.as_server(raft->peers_[raft->peer_id_].m_port.first);
  server.bind("RequestVote", &Raft::RequestVote, raft);

  std::thread(&Raft::ElectionLoop, raft).detach();

  server.run();
  printf("exit!\n");
}

void* Raft::ListenForAppend(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc server;
  server.as_server(raft->peers_[raft->peer_id_].m_port.second);
  server.bind("AppendEntries", &Raft::AppendEntries, raft);
  server.bind("InstallSnapShot", &Raft::InstallSnapShot, raft);

  std::thread(&Raft::ProcessEntriesLoop, raft).detach();

  server.run();
  printf("exit!\n");
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

        pthread_t tid[raft->peers_.size() - 1];
        int i = 0;
        for (auto server : raft->peers_) {
          if (server.peer_id_ == raft->peer_id_) continue;
          pthread_create(tid + i, nullptr, CallRequestVote, raft);
          pthread_detach(tid[i]);
          i++;
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
  args.candidateId = raft->peer_id_;
  args.term = raft->curr_term_;
  args.lastLogIndex = raft->LastIndex();
  args.lastLogTerm = raft->LastTerm();
  if (raft->curr_peer_id_ == raft->peer_id_) {
    raft->curr_peer_id_++;
  }
  int clientPeerId = raft->curr_peer_id_;
  client.as_client("127.0.0.1", raft->peers_[raft->curr_peer_id_++].m_port.first);

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
  if (reply.VoteGranted) {
    raft->recv_votes_++;
  }
}

bool Raft::CheckLogUptodate(int term, int index) {
  int LastTerm = this->LastTerm();
  if (term > LastTerm) {
    return true;
  }
  if (term == LastTerm && index >= LastIndex()) {
    return true;
  }
  return false;
}

RequestVoteReply Raft::RequestVote(RequestVoteArgs args) {
  RequestVoteReply reply;
  reply.VoteGranted = false;
  m_lock.lock();
  reply.term = curr_term_;

  if (curr_term_ > args.term) {
    m_lock.unlock();
    return reply;
  }

  if (curr_term_ < args.term) {
    state_ = FOLLOWER;
    curr_term_ = args.term;
    voted_for_ = -1;
  }

  if (voted_for_ == -1 || voted_for_ == args.candidateId) {
    bool ret = CheckLogUptodate(args.lastLogTerm, args.lastLogIndex);
    if (!ret) {
      m_lock.unlock();
      return reply;
    }
    voted_for_ = args.candidateId;
    reply.VoteGranted = true;
    printf("[%d] vote to [%d] at %d, duration is %d\n", peer_id_,
           args.candidateId, curr_term_, GetMyduration(last_wake_time_));
    ::gettimeofday(&last_wake_time_, nullptr);
  }
  SaveRaftState();
  m_lock.unlock();
  return reply;
}

void* Raft::ProcessEntriesLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  while (!raft->dead_) {
    ::usleep(1000);
    raft->m_lock.lock();
    if (raft->state_ != LEADER) {
      raft->m_lock.unlock();
      continue;
    }
    // printf("sec : %ld, usec : %ld\n", raft->last_broadcast_time_.tv_sec,
    // raft->last_broadcast_time_.tv_usec);
    int during_time = raft->GetMyduration(raft->last_broadcast_time_);
    // printf("time is %d\n", during_time);
    if (during_time < HEART_BEART_PERIOD) {
      raft->m_lock.unlock();
      continue;
    }

    ::gettimeofday(&raft->last_broadcast_time_, nullptr);
    // printf("%d send AppendRetries at %d\n", raft->peer_id_, raft->curr_term_);
    // raft->m_lock.unlock();

    pthread_t tid[raft->peers_.size() - 1];
    int i = 0;
    for (auto& server : raft->peers_) {
      if (server.peer_id_ == raft->peer_id_) continue;
      if (raft->next_index_[server.peer_id_] <=
          raft->last_included_index_) {  //进入install分支的条件，日志落后于leader的快照
        printf(
            "%d send install rpc to %d, whose nextIdx is %d, but leader's "
            "lastincludeIdx is %d\n",
            raft->peer_id_, server.peer_id_, raft->next_index_[server.peer_id_],
            raft->last_included_index_);
        server.isInstallFlag = true;
        pthread_create(tid + i, nullptr, SendInstallSnapShot, raft);
        pthread_detach(tid[i]);
      } else {
        printf(
            "%d send append rpc to %d, whose nextIdx is %d, but leader's "
            "lastincludeIdx is %d\n",
            raft->peer_id_, server.peer_id_, raft->next_index_[server.peer_id_],
            raft->last_included_index_);
        pthread_create(tid + i, nullptr, SendAppendEntries, raft);
        pthread_detach(tid[i]);
      }
      i++;
    }
    raft->m_lock.unlock();
  }
}

void* Raft::SendInstallSnapShot(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  buttonrpc client;
  InstallSnapShotArgs args;
  int clientPeerId;
  raft->m_lock.lock();
  // for(int i = 0; i < raft->peers_.size(); i++){
  //     printf("in install %d's server.isInstallFlag is %d\n", i,
  //     raft->peers_[i].isInstallFlag ? 1 : 0);
  // }
  for (int i = 0; i < raft->peers_.size(); i++) {
    if (raft->peers_[i].peer_id_ == raft->peer_id_) {
      // printf("%d is leader, continue\n", i);
      continue;
    }
    if (!raft->peers_[i].isInstallFlag) {
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

  client.as_client("127.0.0.1", raft->peers_[clientPeerId].m_port.second);

  if (raft->is_exist_index_.size() == raft->peers_.size() - 1) {
    // printf("install clear size is %d\n", raft->is_exist_index_.size());
    for (int i = 0; i < raft->peers_.size(); i++) {
      raft->peers_[i].isInstallFlag = false;
    }
    raft->is_exist_index_.clear();
  }

  args.lastIncludedIndex = raft->last_included_index_;
  args.lastIncludedTerm = raft->last_included_term_;
  args.leaderId = raft->peer_id_;
  args.term = raft->curr_term_;
  raft->ReadSnapShot();
  args.snapShot = raft->persister_.snapShot;

  printf("in send install snapShot is %s\n", args.snapShot.c_str());

  raft->m_lock.unlock();
  // printf("%d send to %d's install port is %d\n", raft->peer_id_,
  // clientPeerId, raft->peers_[clientPeerId].m_port.second);
  InstallSnapSHotReply reply =
      client.call<InstallSnapSHotReply>("InstallSnapShot", args).val();
  // printf("%d is called send install to %d\n", raft->peer_id_, clientPeerId);

  raft->m_lock.lock();
  if (raft->curr_term_ != args.term) {
    raft->m_lock.unlock();
    return nullptr;
  }

  if (raft->curr_term_ < reply.term) {
    raft->state_ = FOLLOWER;
    raft->voted_for_ = -1;
    raft->curr_term_ = reply.term;
    raft->SaveRaftState();
    raft->m_lock.unlock();
    return nullptr;
  }

  raft->next_index_[clientPeerId] = raft->LastIndex() + 1;
  raft->match_index_[clientPeerId] = args.lastIncludedIndex;

  raft->match_index_[raft->peer_id_] = raft->LastIndex();
  std::vector<int> tmpIndex = raft->match_index_;
  sort(tmpIndex.begin(), tmpIndex.end());
  int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
  if (realMajorityMatchIndex > raft->commit_index_ &&
      (realMajorityMatchIndex <= raft->last_included_index_ ||
       raft->logs_[raft->IdxToCompressLogPos(realMajorityMatchIndex)].m_term ==
           raft->curr_term_)) {
    raft->commit_index_ = realMajorityMatchIndex;
  }
  raft->m_lock.unlock();
}

InstallSnapSHotReply Raft::InstallSnapShot(InstallSnapShotArgs args) {
  InstallSnapSHotReply reply;
  m_lock.lock();
  reply.term = curr_term_;

  if (args.term < curr_term_) {
    m_lock.unlock();
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
         args.lastIncludedIndex, last_included_index_, LastIndex());
  if (args.lastIncludedIndex <= last_included_index_) {
    m_lock.unlock();
    return reply;
  } else {
    if (args.lastIncludedIndex < LastIndex()) {
      if (logs_[IdxToCompressLogPos(LastIndex())].m_term !=
          args.lastIncludedTerm) {
        logs_.clear();
      } else {
        std::vector<LogEntry> tmpLog(
            logs_.begin() + IdxToCompressLogPos(args.lastIncludedIndex) + 1,
            logs_.end());
        logs_ = tmpLog;
      }
    } else {
      logs_.clear();
    }
  }

  last_included_index_ = args.lastIncludedIndex;
  last_included_term_ = args.lastIncludedTerm;
  persister_.snapShot = args.snapShot;
  printf("in raft stall rpc, snapShot is %s\n", persister_.snapShot.c_str());
  SaveRaftState();
  SaveSnapShot();

  m_lock.unlock();
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
    int num = atoi(number.c_str());
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
  raft->m_lock.lock();

  // for(int i = 0; i < raft->peers_.size(); i++){
  //     printf("in append %d's server.isInstallFlag is %d\n", i,
  //     raft->peers_[i].isInstallFlag ? 1 : 0);
  // }

  for (int i = 0; i < raft->peers_.size(); i++) {
    if (raft->peers_[i].peer_id_ == raft->peer_id_) continue;
    if (raft->peers_[i].isInstallFlag) continue;
    if (raft->is_exist_index_.count(i)) continue;
    clientPeerId = i;
    raft->is_exist_index_.insert(i);
    // printf("%d in append insert index : %d, size is %d\n", raft->peer_id_, i,
    // raft->is_exist_index_.size());
    break;
  }

  client.as_client("127.0.0.1", raft->peers_[clientPeerId].m_port.second);
  // printf("%d send to %d's append port is %d\n", raft->peer_id_, clientPeerId,
  // raft->peers_[clientPeerId].m_port.second);

  if (raft->is_exist_index_.size() == raft->peers_.size() - 1) {
    // printf("append clear size is %d\n", raft->is_exist_index_.size());
    for (int i = 0; i < raft->peers_.size(); i++) {
      raft->peers_[i].isInstallFlag = false;
    }
    raft->is_exist_index_.clear();
  }

  args.m_term = raft->curr_term_;
  args.leader_id_ = raft->peer_id_;
  args.m_prevLogIndex = raft->next_index_[clientPeerId] - 1;
  args.m_leaderCommit = raft->commit_index_;

  for (int i = raft->IdxToCompressLogPos(args.m_prevLogIndex) + 1;
       i < raft->logs_.size(); i++) {
    args.m_sendLogs += (raft->logs_[i].m_command + "," +
                        to_string(raft->logs_[i].m_term) + ";");
  }

  //用作自己调试可能，因为如果leader的m_prevLogIndex为0，follower的size必为0，自己调试直接赋日志给各个server看选举情况可能需要这段代码
  // if(args.m_prevLogIndex == 0){
  //     args.m_prevLogTerm = 0;
  //     if(raft->logs_.size() != 0){
  //         args.m_prevLogTerm = raft->logs_[0].m_term;
  //     }
  // }

  if (args.m_prevLogIndex == raft->last_included_index_) {
    args.m_prevLogTerm = raft->last_included_term_;
  } else {  //有快照的话m_prevLogIndex必然不为0
    args.m_prevLogTerm =
        raft->logs_[raft->IdxToCompressLogPos(args.m_prevLogIndex)].m_term;
  }

  // printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n",
  // raft->peer_id_, clientPeerId, args.m_prevLogIndex, args.m_prevLogTerm);

  raft->m_lock.unlock();
  AppendEntriesReply reply =
      client.call<AppendEntriesReply>("AppendEntries", args).val();

  raft->m_lock.lock();
  if (raft->curr_term_ != args.m_term) {
    raft->m_lock.unlock();
    return nullptr;
  }
  if (reply.m_term > raft->curr_term_) {
    raft->state_ = FOLLOWER;
    raft->curr_term_ = reply.m_term;
    raft->voted_for_ = -1;
    raft->SaveRaftState();
    raft->m_lock.unlock();
    return nullptr;  // FOLLOWER没必要维护nextIndex,成为leader会更新
  }

  if (reply.m_success) {
    raft->next_index_[clientPeerId] =
        args.m_prevLogIndex + raft->GetCmdAndTerm(args.m_sendLogs).size() +
        1;  //可能RPC调用完log又增加了，但那些是不应该算进去的，不能直接取m_logs.size()
            //+ 1
    raft->match_index_[clientPeerId] = raft->next_index_[clientPeerId] - 1;
    raft->match_index_[raft->peer_id_] = raft->LastIndex();

    std::vector<int> tmpIndex = raft->match_index_;
    sort(tmpIndex.begin(), tmpIndex.end());
    int realMajorityMatchIndex = tmpIndex[tmpIndex.size() / 2];
    if (realMajorityMatchIndex > raft->commit_index_ &&
        (realMajorityMatchIndex <= raft->last_included_index_ ||
         raft->logs_[raft->IdxToCompressLogPos(realMajorityMatchIndex)]
                 .m_term == raft->curr_term_)) {
      raft->commit_index_ = realMajorityMatchIndex;
    }
  }

  if (!reply.m_success) {
    if (reply.m_conflict_term != -1 && reply.m_conflict_term != -100) {
      int leader_conflict_index = -1;
      for (int index = args.m_prevLogIndex; index > raft->last_included_index_;
           index--) {
        if (raft->logs_[raft->IdxToCompressLogPos(index)].m_term ==
            reply.m_conflict_term) {
          leader_conflict_index = index;
          break;
        }
      }
      if (leader_conflict_index != -1) {
        raft->next_index_[clientPeerId] = leader_conflict_index + 1;
      } else {
        raft->next_index_[clientPeerId] =
            reply
                .m_conflict_index;  //这里加不加1都可，无非是多一位还是少一位，此处指follower对应index为空
      }
    } else {
      if (reply.m_conflict_term == -100) {
      }
      //-------------------很关键，运行时不能注释下面这段，因为我自己调试bug强行增加bug，没有专门的测试程序-----------------
      else
        raft->next_index_[clientPeerId] = reply.m_conflict_index;
    }
  }
  raft->SaveRaftState();
  raft->m_lock.unlock();
}

AppendEntriesReply Raft::AppendEntries(AppendEntriesArgs args) {
  std::vector<LogEntry> recvLog = GetCmdAndTerm(args.m_sendLogs);
  AppendEntriesReply reply;
  m_lock.lock();
  reply.m_term = curr_term_;
  reply.m_success = false;
  reply.m_conflict_index = -1;
  reply.m_conflict_term = -1;

  if (args.m_term < curr_term_) {
    m_lock.unlock();
    return reply;
  }

  if (args.m_term >= curr_term_) {
    if (args.m_term > curr_term_) {
      voted_for_ = -1;
      SaveRaftState();
    }
    curr_term_ = args.m_term;
    state_ = FOLLOWER;
  }
  // printf("[%d] recv append from [%d] at self term%d, send term %d, duration
  // is %d\n",
  //         peer_id_, args.leader_id_, curr_term_, args.m_term,
  //         GetMyduration(last_wake_time_));
  ::gettimeofday(&last_wake_time_, nullptr);

  //------------------------------------test----------------------------------
  if (dead_) {
    reply.m_conflict_term = -100;
    m_lock.unlock();
    return reply;
  }
  //------------------------------------test----------------------------------

  if (args.m_prevLogIndex < last_included_index_) {
    printf("[%d]'s last_included_index_ is %d, but args.m_prevLogIndex is %d\n",
           peer_id_, last_included_index_, args.m_prevLogIndex);
    reply.m_conflict_index = 1;
    m_lock.unlock();
    return reply;
  } else if (args.m_prevLogIndex == last_included_index_) {
    printf("[%d]'s last_included_term_ is %d, args.m_prevLogTerm is %d\n",
           peer_id_, last_included_term_, args.m_prevLogTerm);
    if (args.m_prevLogTerm !=
        last_included_term_) {  //脑裂分区，少数派的snapShot不对，回归集群后需要更新自己的snapShot及log
      reply.m_conflict_index = 1;
      m_lock.unlock();
      return reply;
    }
  } else {
    if (LastIndex() < args.m_prevLogIndex) {
      //索引要加1,很关键，避免快照安装一直循环(知道下次快照)，这里加不加1最多影响到回滚次数多一次还是少一次
      //如果不加1，先dead在activate，那么log的size一直都是lastincludedindx，next
      //= conflict = last一直循环， 知道下次超过maxstate，kvserver发起新快照才行
      reply.m_conflict_index = LastIndex() + 1;
      printf(
          " [%d]'s logs.size : %d < [%d]'s prevLogIdx : %d, ret conflict idx "
          "is %d\n",
          peer_id_, LastIndex(), args.leader_id_, args.m_prevLogIndex,
          reply.m_conflict_index);
      m_lock.unlock();
      reply.m_success = false;
      return reply;
    }
    //走到这里必然有日志，且prevLogIndex > 0
    if (logs_[IdxToCompressLogPos(args.m_prevLogIndex)].m_term !=
        args.m_prevLogTerm) {
      printf(" [%d]'s prevLogterm : %d != [%d]'s prevLogTerm : %d\n", peer_id_,
             logs_[IdxToCompressLogPos(args.m_prevLogIndex)].m_term,
             args.leader_id_, args.m_prevLogTerm);

      reply.m_conflict_term =
          logs_[IdxToCompressLogPos(args.m_prevLogIndex)].m_term;
      for (int index = last_included_index_ + 1; index <= args.m_prevLogIndex;
           index++) {
        if (logs_[IdxToCompressLogPos(index)].m_term ==
            reply.m_conflict_term) {
          reply.m_conflict_index =
              index;  //找到冲突term的第一个index,比索引要加1
          break;
        }
      }
      m_lock.unlock();
      reply.m_success = false;
      return reply;
    }
  }
  //走到这里必然PrevLogterm与对应follower的index处term相等，进行日志覆盖
  int logSize = LastIndex();
  for (int i = args.m_prevLogIndex; i < logSize; i++) {
    logs_.pop_back();
  }
  // logs_.insert(logs_.end(), recvLog.begin(), recvLog.end());
  for (const auto& log : recvLog) {
    PushBackLog(log);
  }
  SaveRaftState();
  if (commit_index_ < args.m_leaderCommit) {
    commit_index_ = min(args.m_leaderCommit, LastIndex());
    // commit_index_ = args.m_leaderCommit;
  }
  // for(auto a : logs_) printf("%d ", a.m_term);
  // printf(" [%d] sync success\n", peer_id_);
  m_lock.unlock();
  reply.m_success = true;
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
  m_lock.lock();
  RAFT_STATE state = state_;
  if (state != LEADER) {
    // printf("index : %d, term : %d, isleader : %d\n", ret.m_cmdIndex,
    // ret.curr_term_, ret.isLeader == false ? 0 : 1);
    m_lock.unlock();
    return ret;
  }

  LogEntry log;
  log.m_command = op.getCmd();
  log.m_term = curr_term_;
  PushBackLog(log);

  ret.m_cmdIndex = LastIndex();
  ret.curr_term_ = curr_term_;
  ret.isLeader = true;
  // printf("index : %d, term : %d, isleader : %d\n", ret.m_cmdIndex,
  // ret.curr_term_, ret.isLeader == false ? 0 : 1);
  m_lock.unlock();

  return ret;
}

void Raft::PrintLogs() {
  for (auto a : logs_) {
    printf("logs : %d\n", a.m_term);
  }
  cout << endl;
}

void Raft::Serialize() {
  std::string str;
  str += to_string(this->persister_.cur_term) + ";" +
         to_string(this->persister_.votedFor) + ";";
  str += to_string(this->persister_.lastIncludedIndex) + ";" +
         to_string(this->persister_.lastIncludedTerm) + ";";
  for (const auto& log : this->persister_.logs) {
    str += log.m_command + "," + to_string(log.m_term) + ".";
  }
  std::string filename = "persister_-" + to_string(peer_id_);
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
  int len = write(fd, str.c_str(), str.size());
  close(fd);
}

bool Raft::Deserialize() {
  std::string filename = "persister_-" + to_string(peer_id_);
  if (access(filename.c_str(), F_OK) == -1) return false;
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("open");
    return false;
  }
  int length = lseek(fd, 0, SEEK_END);
  lseek(fd, 0, SEEK_SET);
  char buf[length];
  bzero(buf, length);
  int len = read(fd, buf, length);
  if (len != length) {
    perror("read");
    exit(-1);
  }
  close(fd);
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
  this->persister_.cur_term = atoi(persist[0].c_str());
  this->persister_.votedFor = atoi(persist[1].c_str());
  this->persister_.lastIncludedIndex = atoi(persist[2].c_str());
  this->persister_.lastIncludedTerm = atoi(persist[3].c_str());
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
    int num = atoi(number.c_str());
    logs.push_back(LogEntry(tmp, num));
  }
  this->persister_.logs = logs;
  return true;
}

void Raft::ReadRaftState() {
  //只在初始化的时候调用，没必要加锁，因为run()在其之后才执行
  bool ret = this->Deserialize();
  if (!ret) return;
  this->curr_term_ = this->persister_.cur_term;
  this->voted_for_ = this->persister_.votedFor;

  for (const auto& log : this->persister_.logs) {
    PushBackLog(log);
  }
  printf(" [%d]'s term : %d, votefor : %d, logs.size() : %d\n", peer_id_,
         curr_term_, voted_for_, logs_.size());
}

void Raft::SaveRaftState() {
  persister_.cur_term = curr_term_;
  persister_.votedFor = voted_for_;
  persister_.logs = logs_;
  persister_.lastIncludedIndex = this->last_included_index_;
  persister_.lastIncludedTerm = this->last_included_term_;
  Serialize();
}

void Raft::SetSendSem(int num) { m_sendSem.init(num); }
void Raft::SetRecvSem(int num) { m_recvSem.init(num); }

bool Raft::WaitSendSem() { return m_sendSem.wait(); }
bool Raft::WaitRecvSem() { return m_recvSem.wait(); }
bool Raft::PostSendSem() { return m_sendSem.post(); }
bool Raft::PostRecvSem() { return m_recvSem.post(); }

ApplyMsg Raft::GetBackMsg() { return msgs_.back(); }

bool Raft::ExceedLogSize(int size) {
  bool ret = false;

  m_lock.lock();
  int sum = 8;
  for (int i = 0; i < persister_.logs.size(); i++) {
    sum += persister_.logs[i].m_command.size() + 3;
  }
  ret = (sum >= size ? true : false);
  if (ret) printf("[%d] in Exceed the log size is %d\n", peer_id_, sum);
  m_lock.unlock();

  return ret;
}

void Raft::RecvSnapShot(std::string snapShot, int lastIncludedIndex) {
  m_lock.lock();

  if (lastIncludedIndex < this->last_included_index_) {
    return;
  }
  int compressLen = lastIncludedIndex - this->last_included_index_;
  printf(
      "[%d] before log.size is %d, compressLen is %d, lastIncludedIndex is "
      "%d\n",
      peer_id_, logs_.size(), compressLen, last_included_index_);

  printf("[%d] : %d - %d = compressLen is %d\n", peer_id_, lastIncludedIndex,
         this->last_included_index_, compressLen);
  this->last_included_term_ =
      logs_[IdxToCompressLogPos(lastIncludedIndex)].m_term;
  this->last_included_index_ = lastIncludedIndex;

  std::vector<LogEntry> tmpLogs;
  for (int i = compressLen; i < logs_.size(); i++) {
    tmpLogs.push_back(logs_[i]);
  }
  logs_ = tmpLogs;
  printf("[%d] after log.size is %d\n", peer_id_, logs_.size());
  //更新了logs及lastTerm和lastIndex，需要持久化
  persister_.snapShot = snapShot;
  SaveRaftState();

  SaveSnapShot();
  printf("[%d] persister_.size is %d, lastIncludedIndex is %d\n", peer_id_,
         persister_.logs.size(), last_included_index_);
  m_lock.unlock();
}

int Raft::IdxToCompressLogPos(int index) {
  return index - this->last_included_index_ - 1;
}

bool Raft::ReadSnapShot() {
  std::string filename = "snapShot-" + to_string(peer_id_);
  if (access(filename.c_str(), F_OK) == -1) return false;
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("open");
    return false;
  }
  int length = lseek(fd, 0, SEEK_END);
  lseek(fd, 0, SEEK_SET);
  char buf[length];
  bzero(buf, length);
  int len = read(fd, buf, length);
  if (len != length) {
    perror("read");
    exit(-1);
  }
  close(fd);
  std::string snapShot(buf);
  persister_.snapShot = snapShot;
  return true;
}

void Raft::SaveSnapShot() {
  std::string filename = "snapShot-" + to_string(peer_id_);
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
  int len =
      write(fd, persister_.snapShot.c_str(), persister_.snapShot.size() + 1);
  close(fd);
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
  m_lock.lock();
  bool ret = ReadSnapShot();

  if (!ret) {
    // installSnapShotFlag = false;
    // applyLogFlag = true;
    m_lock.unlock();
    return;
  }

  ApplyMsg msg;
  msg.commandValid = false;
  msg.snapShot = persister_.snapShot;
  msg.lastIncludedIndex = this->last_included_index_;
  msg.lastIncludedTerm = this->last_included_term_;

  last_applied_ = last_included_index_;
  m_lock.unlock();

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
    LastTerm = logs_.back().m_term;
  }
  return LastTerm;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("loss parameter of peersNum\n");
    exit(-1);
  }
  int peersNum = atoi(argv[1]);
  if (peersNum % 2 == 0) {
    printf("the peersNum should be odd\n");
    exit(-1);
  }
  srand((unsigned)time(nullptr));
  std::vector<PeersInfo> peers(peersNum);
  for (int i = 0; i < peersNum; i++) {
    peers[i].peer_id_ = i;
    peers[i].m_port.first = COMMOM_PORT + i;
    peers[i].m_port.second = COMMOM_PORT + i + peers.size();
    // printf(" id : %d port1 : %d, port2 : %d\n", peers[i].peer_id_,
    // peers[i].m_port.first, peers[i].m_port.second);
  }

  Raft* raft = new Raft[peers.size()];
  for (int i = 0; i < peers.size(); i++) {
    raft[i].Make(peers, i);
  }
  ::usleep(400000);
  for (int i = 0; i < peers.size(); i++) {
    if (raft[i].GetState().second) {
      for (int j = 0; j < 1000; j++) {
        Operation opera;
        opera.op = "put";
        opera.key = to_string(j);
        opera.value = to_string(j);
        raft[i].Start(opera);
        ::usleep(50000);
      }
    } else
      continue;
  }
  ::usleep(400000);
  for (int i = 0; i < peers.size(); i++) {
    if (raft[i].GetState().second) {
      raft[i].Kill();
      break;
    }
  }

  while (1)
    ;
}