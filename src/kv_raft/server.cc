#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_raft/arguments.hpp"
#include "kv_raft/raft.hpp"

constexpr int EVERY_SERVER_PORT = 3;

using MyClock = std::chrono::steady_clock;
using MyTime = std::chrono::steady_clock::time_point;

constexpr std::chrono::microseconds MyDuration(const MyClock::duration& d) {
  return std::chrono::duration_cast<std::chrono::microseconds>(d);
}

struct KVServerInfo {
  PeersInfo peers_info;
  std::vector<int> kv_ports;
};

class KVServer {
 public:
  void RPCServer();
  void ApplyLoop();  //持续监听raft层提交的msg的守护线程
  void SnapShotLoop();  //持续监听raft层日志是否超过给定大小，判断进行快照的守护线程
  void StartKvServer(std::vector<KVServerInfo>& kv_info, int me,
                     int maxRaftState);
  static std::vector<PeersInfo> GetRaftPort(std::vector<KVServerInfo>& kv_info);
  GetReply Get(const GetArgs& args);
  PutAppendReply PutAppend(const PutAppendArgs& args);

  std::string Test(const std::string& key) {
    return database_[key];
  }  //测试其余不是leader的server的状态机

  std::string GetSnapShot();  //将kvServer的状态信息转化为snapShot
  void RecoverySnapShot(
      const std::string&
          snapshot);  //将从raft层获得的快照安装到kvServer即应用层中(必然已经落后其他的server了，或者是初始化)

  //---------------------------Test----------------------------
  bool GetRaftState();  //获取raft状态
  void KillRaft();  //测试安装快照功能时使用，让raft暂停接受日志
  void ActivateRaft();  //重新激活raft的功能

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  Raft raft_;
  int id_{};
  std::vector<int> port_;
  int curr_port_id_{};

  // bool dead;

  int max_raft_state_{};  //超过这个大小就快照
  int last_applied_index_{};

  std::unordered_map<std::string, std::string> database_;  //模拟数据库
  std::unordered_map<int, int>
      client_seq_map_;  //只记录特定客户端已提交的最大请求ID
  std::unordered_map<int, OpContext*> request_map_;  //记录当前RPC对应的上下文
};

void KVServer::StartKvServer(std::vector<KVServerInfo>& kv_info, int me,
                             int maxRaftState) {
  this->id_ = me;
  port_ = kv_info[me].kv_ports;
  std::vector<PeersInfo> peers = GetRaftPort(kv_info);
  this->max_raft_state_ = maxRaftState;
  last_applied_index_ = 0;

  raft_.SetRecvSem(1);
  raft_.SetSendSem(0);
  raft_.Make(peers, me);

  database_.clear();
  client_seq_map_.clear();
  request_map_.clear();

  // dead = false;

  for (int i = 0; i < port_.size(); i++) {
    std::thread(&KVServer::RPCServer, this).detach();
  }
  std::thread(&KVServer::ApplyLoop, this).detach();
  std::thread(&KVServer::SnapShotLoop, this).detach();
}

void KVServer::RPCServer() {
  buttonrpc server;
  std::unique_lock<std::mutex> lock(mutex_);
  int port = curr_port_id_++;
  lock.unlock();

  server.as_server(port_[port]);
  server.bind("Get", &KVServer::Get, this);
  server.bind("PutAppend", &KVServer::PutAppend, this);
  server.run();
}

// PRChandler for Get-request
GetReply KVServer::Get(const GetArgs& args) {
  GetReply reply;
  reply.is_wrong_leader = false;
  reply.isKeyExist = true;
  Operation operation;
  operation.op = "Get";
  operation.key = args.key;
  operation.value = "random";
  operation.client_id = args.client_id;
  operation.request_id = args.request_id;

  StartRet ret = raft_.Start(operation);
  operation.term = ret.curr_term;
  operation.index = ret.cmd_index;

  if (ret.is_leader == false) {
    printf("client %d's Get request is wrong leader %d\n", args.client_id, id_);
    reply.is_wrong_leader = true;
    return reply;
  }

  OpContext opctx(
      operation);  //创建RPC时的上下文信息并暂存到map中，其key为start返回的该条请求在raft日志中唯一的索引
  std::unique_lock<std::mutex> lock(mutex_);
  request_map_[ret.cmd_index] = &opctx;
  lock.unlock();
  Select s(opctx.fifo_name);  //创建监听管道数据的定时对象
  MyTime curTime = MyClock::now();
  while (MyDuration(MyClock::now() - curTime).count() < 2000000) {
    if (s.is_recved) {
      // printf("client %d's Get->time is %d\n", args.clientId,
      // MyDuration(MyClock::now() - curTime).count());
      break;
    }
    ::usleep(10000);
  }

  if (s.is_recved) {
    if (opctx.is_wrong_leader) {
      reply.is_wrong_leader = true;
    } else if (!opctx.is_key_existed) {
      reply.isKeyExist = false;
    } else {
      // printf("Get hit the key, value is %s\n", opctx.value.c_str());
      reply.value = opctx.value;
      // printf("Get hit the key, reply is %s\n", reply.value.c_str());
    }
  } else {
    reply.is_wrong_leader = true;
    printf("in Get --------- timeout!!!\n");
  }
  lock.lock();
  request_map_.erase(ret.cmd_index);
  lock.unlock();
  return reply;
}

// PRChandler for Put/Append-request
PutAppendReply KVServer::PutAppend(const PutAppendArgs& args) {
  PutAppendReply reply{};
  reply.is_wrong_leader = false;
  Operation operation;
  operation.op = args.op;
  operation.key = args.key;
  operation.value = args.value;
  operation.client_id = args.client_id;
  operation.request_id = args.request_id;

  StartRet ret = raft_.Start(operation);

  operation.term = ret.curr_term;
  operation.index = ret.cmd_index;
  if (ret.is_leader == false) {
    printf("client %d's PutAppend request is wrong leader %d\n", args.client_id,
           id_);
    reply.is_wrong_leader = true;
    return reply;
  }

  OpContext opctx(
      operation);  //创建RPC时的上下文信息并暂存到map中，其key为start返回的该条请求在raft日志中唯一的索引
  std::unique_lock<std::mutex> lock(mutex_);
  request_map_[ret.cmd_index] = &opctx;
  lock.unlock();

  Select s(opctx.fifo_name);  //创建监听管道数据的定时对象
  MyTime curTime = MyClock::now();
  while (MyDuration(MyClock::now() - curTime).count() < 2000000) {
    if (s.is_recved) {
      // printf("client %d's PutAppend->time is %d\n", args.clientId,
      // MyDuration(MyClock::now() - curTime).count());
      break;
    }
    ::usleep(10000);
  }

  if (s.is_recved) {
    // printf("opctx.isWrongLeader : %d\n", opctx.isWrongLeader ? 1 : 0);
    if (opctx.is_wrong_leader) {
      reply.is_wrong_leader = true;
    } else if (opctx.is_ignored) {
      //啥也不管即可，请求过期需要被忽略，返回ok让客户端不管即可
    }
  } else {
    reply.is_wrong_leader = true;
    printf("int PutAppend --------- timeout!!!\n");
  }
  lock.lock();
  request_map_.erase(ret.cmd_index);
  // lock.unlock();
  return reply;
}

void KVServer::ApplyLoop() {
  while (true) {
    raft_.WaitSendSem();
    ApplyMsg msg = raft_.GetBackMsg();

    if (!msg.is_command_valid) {  //为快照处理的逻辑
      std::lock_guard<std::mutex> lock(mutex_);
      if (msg.snapshot.empty()) {
        database_.clear();
        client_seq_map_.clear();
      } else {
        RecoverySnapShot(msg.snapshot);
      }
      //一般初始化时安装快照，以及follower收到installSnapShot向上层kvserver发起安装快照请求
      last_applied_index_ = msg.last_included_index;
      printf("in stall last_applied_index_ is %d\n", last_applied_index_);
    } else {
      Operation operation = msg.GetOperation();
      int index = msg.command_index;

      std::unique_lock<std::mutex> lock(mutex_);
      last_applied_index_ = index;  //收到一个msg就更新m_lastAppliedIndex
      bool isOpExist = false, isSeqExist = false;
      int prevRequestIdx = std::numeric_limits<int>::max();  // INT_MAX;
      OpContext* opctx = nullptr;
      if (request_map_.count(index)) {
        isOpExist = true;
        opctx = request_map_[index];
        if (opctx->op.term != operation.term) {
          opctx->is_wrong_leader = true;
          printf("not euqal term -> wrongLeader : opctx %d, op : %d\n",
                 opctx->op.term, operation.term);
        }
      }
      if (client_seq_map_.count(operation.client_id)) {
        isSeqExist = true;
        prevRequestIdx = client_seq_map_[operation.client_id];
      }
      client_seq_map_[operation.client_id] = operation.request_id;

      if (operation.op == "Put" || operation.op == "Append") {
        //非leader的server必然不存在命令，同样处理状态机，leader的第一条命令也不存在，保证按序处理
        if (!isSeqExist || prevRequestIdx < operation.request_id) {
          if (operation.op == "Put") {
            database_[operation.key] = operation.value;
          } else if (operation.op == "Append") {
            if (database_.count(operation.key)) {
              database_[operation.key] += operation.value;
            } else {
              database_[operation.key] = operation.value;
            }
          }
        } else if (isOpExist) {
          opctx->is_ignored = true;
        }
      } else {
        if (isOpExist) {
          if (database_.count(operation.key)) {
            opctx->value = database_[operation.key];  //如果有则返回value
          } else {
            opctx->is_key_existed = false;
            opctx->value = "";  //如果无返回""
          }
        }
      }

      lock.unlock();

      //保证只有存了上下文信息的leader才能唤醒管道，回应clerk的RPC请求(leader需要多做的工作)
      if (isOpExist) {
        int fd = ::open(opctx->fifo_name.c_str(), O_WRONLY);
        char buf[] = "12345";
        ::write(fd, buf, strlen(buf) + 1);
        ::close(fd);
      }
    }
    raft_.PostRecvSem();
  }
}

std::string KVServer::GetSnapShot() {
  std::string snapshot;
  for (const auto& ele : database_) {
    snapshot += ele.first + " " + ele.second + ".";
  }
  snapshot += ";";
  for (const auto& ele : client_seq_map_) {
    snapshot +=
        std::to_string(ele.first) + " " + std::to_string(ele.second) + ".";
  }
  std::cout << "int std::cout snapshot is " << snapshot << std::endl;
  printf("in kvserver -----------------snapshot is %s\n", snapshot.c_str());
  return snapshot;
}

void KVServer::SnapShotLoop() {
  while (true) {
    std::string snapshot;
    int lastIncluedIndex;
    // printf("%d not in loop -> last_applied_index_ : %d\n", id_,
    // last_applied_index_);
    if (max_raft_state_ != -1 &&
        raft_.ExceedLogSize(
            max_raft_state_)) {  //设定了大小且超出大小则应用层进行快照
      std::lock_guard<std::mutex> lock(mutex_);
      snapshot = GetSnapShot();
      lastIncluedIndex = last_applied_index_;
      // printf("%d in loop -> last_applied_index_ : %d\n", id_,
      // last_applied_index_);
    }
    if (!snapshot.empty()) {
      raft_.RecvSnapShot(
          snapshot,
          lastIncluedIndex);  //向raft层发送快照用于日志压缩，同时持久化
      printf("%d called recvsnapShot size is %d, lastapply is %d\n", id_,
             static_cast<int>(snapshot.size()), last_applied_index_);
    }
    ::usleep(10000);
  }
}

std::vector<KVServerInfo> GetKvServerPort(int num) {
  std::vector<KVServerInfo> peers(num);
  for (int i = 0; i < num; i++) {
    peers[i].peers_info.peer_id = i;
    peers[i].peers_info.port.first = COMMOM_PORT + i;
    peers[i].peers_info.port.second = COMMOM_PORT + i + num;
    peers[i].peers_info.is_install_flag = false;
    for (int j = 0; j < EVERY_SERVER_PORT; j++) {
      peers[i].kv_ports.push_back(COMMOM_PORT + i + (j + 2) * num);
    }
    // printf(" id : %d port1 : %d, port2 : %d\n", peers[i].m_peerId,
    // peers[i].port_.first, peers[i].port_.second);
  }
  return peers;
}

std::vector<PeersInfo> KVServer::GetRaftPort(
    std::vector<KVServerInfo>& kv_info) {
  size_t n = kv_info.size();
  std::vector<PeersInfo> ret(n);
  for (int i = 0; i < n; i++) {
    ret[i] = kv_info[i].peers_info;
  }
  return ret;
}

void KVServer::RecoverySnapShot(const std::string& snapshot) {
  printf("recovery is called\n");
  std::vector<std::string> strs;
  std::string tmp;
  for (char c : snapshot) {
    if (c != ';') {
      tmp += c;
    } else {
      if (!tmp.empty()) {
        strs.push_back(tmp);
        tmp = "";
      }
    }
  }
  if (!tmp.empty()) strs.push_back(tmp);
  tmp = "";
  std::vector<std::string> kv_datas, clients_seq;
  for (int i = 0; i < strs[0].size(); i++) {
    if (strs[0][i] != '.') {
      tmp += strs[0][i];
    } else {
      if (!tmp.empty()) {
        kv_datas.push_back(tmp);
        tmp = "";
      }
    }
  }
  for (int i = 0; i < strs[1].size(); i++) {
    if (strs[1][i] != '.') {
      tmp += strs[1][i];
    } else {
      if (!tmp.empty()) {
        clients_seq.push_back(tmp);
        tmp = "";
      }
    }
  }
  for (auto& kv_data : kv_datas) {
    tmp = "";
    int j = 0;
    for (; j < kv_data.size(); j++) {
      if (kv_data[j] != ' ') {
        tmp += kv_data[j];
      } else
        break;
    }
    std::string value(kv_data.begin() + j + 1, kv_data.end());
    database_[tmp] = value;
  }
  for (auto& client : clients_seq) {
    tmp = "";
    int j = 0;
    for (; j < client.size(); j++) {
      if (client[j] != ' ') {
        tmp += client[j];
      } else
        break;
    }
    std::string value(client.begin() + j + 1, client.end());
    client_seq_map_[std::stoi(tmp)] = std::stoi(value);
  }
  printf("-----------------databegin---------------------------\n");
  for (const auto& a : database_) {
    printf("data-> key is %s, value is %s\n", a.first.c_str(),
           a.second.c_str());
  }
  printf("-----------------requSeqbegin---------------------------\n");
  for (auto a : client_seq_map_) {
    printf("data-> key is %d, value is %d\n", a.first, a.second);
  }
}

bool KVServer::GetRaftState() { return raft_.GetState().second; }

void KVServer::KillRaft() { raft_.Kill(); }

void KVServer::ActivateRaft() { raft_.Activate(); }

int main() {
  std::vector<KVServerInfo> servers = GetKvServerPort(5);
  std::srand((unsigned)std::time(nullptr));

  std::vector<std::unique_ptr<KVServer>> kv_servers;
  kv_servers.reserve(servers.size());
  for (int i = 0; i < servers.size(); ++i) {
    kv_servers.emplace_back(std::make_unique<KVServer>());
    kv_servers[i]->StartKvServer(servers, i, 1024);
  }

  //--------------------------------------Test---------------------------------------------
  ::sleep(3);
  for (int i = 0; i < 5; i++) {
    printf("server%d's key : abc -> value is %s\n", i,
           kv_servers[i]->Test("abc").c_str());
  }
  ::sleep(5);
  int i = 2;
  while (true) {
    i = rand() % 5;
    if (!kv_servers[i]->GetRaftState()) {
      kv_servers[i]
          ->KillRaft();  //先让某个不是leader的raft宕机，不接受leader的appendEntriesRPC，让日志落后于leader的快照状态
      break;
    }
  }
  ::sleep(3);
  kv_servers[i]
      ->ActivateRaft();  //重新激活对应的raft，在raft层发起installRPC请求，且向对应落后的kvServer安装从raft的leader处获得的快照
  //--------------------------------------Test---------------------------------------------
  while (true)
    ;
}
