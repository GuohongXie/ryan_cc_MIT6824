#ifndef RYAN_DS_KV_RAFT_ARGUMENTS_H_
#define RYAN_DS_KV_RAFT_ARGUMENTS_H_

#include <sys/stat.h>
#include <sys/types.h>

#include "kv_raft/raft.hpp"

//用于定时的类，创建一个有名管道，若在指定时间内收到msg则处理业务逻辑，不然按照超时处理重试
struct Select {
  Select(std::string fifo_name);
  std::string fifo_name;
  bool is_recved;
  static void* Work(void* arg);
};

Select::Select(std::string fifo_name) {
  this->fifo_name = fifo_name;
  is_recved = false;
  int ret = ::mkfifo(fifo_name.c_str(), 0664);
  std::thread(&Select::Work, this).detach();
  // pthread_t test_tid;
  // pthread_create(&test_tid, NULL, Work, this);
  // pthread_detach(test_tid);
}

void* Select::Work(void* arg) {
  Select* select = (Select*)arg;
  char buf[100];
  int fd = ::open(select->fifo_name.c_str(), O_RDONLY);
  ::read(fd, buf, sizeof(buf));
  select->is_recved = true;
  ::close(fd);
  ::unlink(select->fifo_name.c_str());
}

//用于保存处理客户端RPC请求时的上下文信息，每次调用start()且为leader时会存到对应的map中，key为start返回的日志index，独一无二
struct OpContext {
  OpContext(Operation op);
  Operation op;
  std::string fifo_name;  //对应当前上下文的有名管道名称
  bool is_wrong_leader;
  bool is_ignored;

  //针对get请求
  bool is_key_existed;
  std::string value;
};

OpContext::OpContext(Operation op) {
  this->op = op;
  fifo_name = "fifo-" + std::to_string(op.client_id) + +"-" +
              std::to_string(op.request_id);
  is_wrong_leader = false;
  is_ignored = false;
  is_key_existed = true;
  value = "";
}

struct GetArgs {
  std::string key;
  int client_id;
  int request_id;
  friend Serializer& operator>>(Serializer& in, GetArgs& d) {
    in >> d.key >> d.client_id >> d.request_id;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, GetArgs d) {
    out << d.key << d.client_id << d.request_id;
    return out;
  }
};

struct GetReply {
  std::string value;
  bool is_wrong_leader;
  bool isKeyExist;
  friend Serializer& operator>>(Serializer& in, GetReply& d) {
    in >> d.value >> d.is_wrong_leader >> d.isKeyExist;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, GetReply d) {
    out << d.value << d.is_wrong_leader << d.isKeyExist;
    return out;
  }
};

struct PutAppendArgs {
  std::string key;
  std::string value;
  std::string op;
  int client_id;
  int request_id;
  friend Serializer& operator>>(Serializer& in, PutAppendArgs& d) {
    in >> d.key >> d.value >> d.op >> d.client_id >> d.request_id;
    return in;
  }
  friend Serializer& operator<<(Serializer& out, PutAppendArgs d) {
    out << d.key << d.value << d.op << d.client_id << d.request_id;
    return out;
  }
};

struct PutAppendReply {
  bool is_wrong_leader;
};

#endif  // RYAN_DS_KV_RAFT_ARGUMENTS_H_