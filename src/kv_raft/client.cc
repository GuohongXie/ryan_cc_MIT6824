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
#include "kv_raft/arguments.hpp"


constexpr int EVERY_SERVER_PORT = 3;

//为了减轻server端的RPC压力，所以server对PUT,GET,APPEND操作设置了多个RPC端口响应
int curr_port_id = 0;  

//由于applyLoop中做了处理，只响应递增请求，满足线性一致性，
//且applyLoop是做完一个在做下一个
//即完全按照raft日志提交顺序做，客户端并发虽然不能判断哪个先写入日志，
//但能保证看到的一定是满足按照日志应用的结果
std::mutex port_mutex;  

class Clerk {
 public:
  explicit Clerk(std::vector<std::vector<int>>& servers);
  std::string Get(std::string key);  //定义的对于kvServer的get请求
  void Put(std::string key, std::string value);  //定义的对于kvServer的put请求
  void Append(std::string key,
              std::string value);  //定义的对于kvServer的append请求
  void PutAppend(std::string key, std::string value,
                 std::string op);  // put、append统一处理函数
  int GetCurRequestId();
  int GetCurLeader();
  int GetChangeLeader();

 private:
  std::mutex mutex_;
  std::vector<std::vector<int>> servers_;
  int leader_id_;   //暂存的leaderID，不用每次都轮询一遍
  int client_id_;   //独一无二的客户端ID
  int request_id_;  //只会递增的该客户端的请求ID，保证按序执行
};

Clerk::Clerk(std::vector<std::vector<int>>& servers) {
  this->servers_ = servers;
  this->client_id_ = std::rand() % 10000 + 1;
  printf("client_id_ is %d\n", client_id_);
  this->request_id_ = 0;
  this->leader_id_ = std::rand() % servers.size();
}

std::string Clerk::Get(std::string key) {
  GetArgs args;
  args.key = std::move(key);
  args.client_id = client_id_;
  args.request_id = GetCurRequestId();
  int cur_leader = GetCurLeader();

  std::unique_lock<std::mutex> lock(port_mutex);
  int curPort =
      (curr_port_id++) %
      EVERY_SERVER_PORT;  //取得某个kvServer的一个RPC监听端口号的索引，一个Server有多个RPC处理客户端请求，取完递增
  lock.unlock();

  while (true) {
    buttonrpc client;
    client.as_client("127.0.0.1", servers_[cur_leader][curPort]);
    GetReply reply = client.call<GetReply>("Get", args)
                         .val();  //取得RPCreply，对于get需要有返回值value
    if (reply.is_wrong_leader) {
      cur_leader = GetChangeLeader();
      ::usleep(1000);
    } else {
      if (reply.isKeyExist) {
        return reply.value;
      } else {
        return "";
      }
    }
  }
}

//取得当前clerk的请求号，取出来就递增
int Clerk::GetCurRequestId() {  //封装成原子操作，避免每次加解锁，代码复用
  std::lock_guard<std::mutex> lock(mutex_);
  int cur_requestId = request_id_++;
  return cur_requestId;
}

//取得当前暂存的kvServerLeaderID
int Clerk::GetCurLeader() {
  std::lock_guard<std::mutex> lock(mutex_);
  int cur_leader = leader_id_;
  return cur_leader;
}

// leader不对更换leader
int Clerk::GetChangeLeader() {
  std::lock_guard<std::mutex> lock(mutex_);
  leader_id_ = (leader_id_ + 1) % servers_.size();
  int new_leader = leader_id_;
  return new_leader;
}

void Clerk::Put(std::string key, std::string value) {
  PutAppend(key, value, "Put");
}

void Clerk::Append(std::string key, std::string value) {
  PutAppend(key, value, "Append");
}

void Clerk::PutAppend(std::string key, std::string value, std::string op) {
  PutAppendArgs args;
  args.key = key;
  args.value = value;
  args.op = op;
  args.client_id = client_id_;
  args.request_id = GetCurRequestId();
  int cur_leader = GetCurLeader();

  std::unique_lock<std::mutex> lock(port_mutex);
  int curPort = (curr_port_id++) % EVERY_SERVER_PORT;
  lock.unlock();

  while (true) {
    buttonrpc client;
    client.as_client("127.0.0.1", servers_[cur_leader][curPort]);
    PutAppendReply reply =
        client.call<PutAppendReply>("PutAppend", args)
            .val();  //取得RPCreply，对于put、append只需知道是否成功，直到成功才停止
    if (!reply.is_wrong_leader) {
      return;
    }
    printf("clerk%d's leader %d is wrong\n", client_id_, cur_leader);
    cur_leader = GetChangeLeader();
    ::usleep(1000);
  }
}

std::vector<std::vector<int>> GetServerPort(int num) {
  std::vector<std::vector<int>> kv_server_ports(num);
  for (int i = 0; i < num; i++) {
    for (int j = 0; j < 3; j++) {
      kv_server_ports[i].push_back(COMMOM_PORT + i + (j + 2) * num);
    }
  }
  return kv_server_ports;
}

int main() {
  std::srand((unsigned)std::time(nullptr));
  std::vector<std::vector<int>> port = GetServerPort(5);
  // printf("server.size() = %d\n", port.size());
  Clerk clerk(port);
  Clerk clerk2(port);
  Clerk clerk3(port);
  Clerk clerk4(port);
  Clerk clerk5(port);

  //-------------------------------------test-------------------------------------
  while (true) {
    clerk.Put("abc", "123");
    std::cout << clerk.Get("abc") << std::endl;
    clerk2.Put("abc", "456");
    clerk3.Append("abc", "789");
    std::cout << clerk.Get("abc") << std::endl;
    clerk4.Put("bcd", "111");
    std::cout << clerk.Get("bcd") << std::endl;
    clerk5.Append("bcd", "222");
    std::cout << clerk3.Get("bcd") << std::endl;
    std::cout << clerk3.Get("abcd") << std::endl;
    clerk5.Append("bcd", "222");
    clerk4.Append("bcd", "222");
    std::cout << clerk2.Get("bcd") << std::endl;
    clerk3.Append("bcd", "222");
    clerk2.Append("bcd", "232");
    std::cout << clerk4.Get("bcd") << std::endl;
    clerk.Append("bcd", "222");
    clerk4.Put("bcd", "111");
    std::cout << clerk3.Get("bcd") << std::endl;
    ::usleep(10000);
  }
  //-------------------------------------test-------------------------------------
}
