#ifndef RYAN_DS_KV_RAFT_SELECT_H_
#define RYAN_DS_KV_RAFT_SELECT_H_

#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#include "locker.h"
using namespace std;

class Select {
 public:
  Select(std::string fifo_name);
  int timeout;
  std::string fifo_name;
  char op;
  locker m_lock;
  cond m_cond;
  std::mutex mutex_;
  std::condition_variable cond_;
  void MySelect();
  static void* WaitTime(void* arg);
  static void* Work(void* arg);
  static void* SetTimeOut(void* arg);
  static void* Send(void* arg);
  static void* Test(void* arg);
};

Select::Select(std::string fifo_name) {
  this->fifo_name = fifo_name;
  int ret = ::mkfifo(fifo_name.c_str(), 0664);
  std::thread(&Select::Send, this).detach();
  std::thread(&Select::Test, this).detach();
}

void* Select::Send(void* arg) {
  Select* s = static_cast<Select*>(arg);
  int fd = ::open(s->fifo_name.c_str(), O_WRONLY);
  char* buf = "12345";
  ::sleep(1);
  ::write(fd, buf, strlen(buf) + 1);
}

void* Select::WaitTime(void* arg) { ::sleep(2); }

void* Select::SetTimeOut(void* arg) {
  Select* select = static_cast<Select*>(arg);
  pthread_t tid;
  std::thread t(WaitTime, nullptr);
  t.join();
  std::unique_lock<std::mutex> lock(select->mutex_);
  select->op = '1';
  select->cond_.notify_one();
}

void* Select::Work(void* arg) {
  Select* select = static_cast<Select*>(arg);
  char buf[100];
  int fd = ::open(select->fifo_name.c_str(), O_RDONLY);
  ::read(fd, buf, sizeof(buf));
  std::unique_lock<std::mutex> lock(select->mutex_);
  select->op = '2';
  select->m_cond.signal();
}

void Select::MySelect() {
  std::thread(&Select::SetTimeOut, this).detach();
  std::thread(&Select::Work, this).detach();
  std::unique_lock<std::mutex> lock(mutex_);
  cond_.wait(lock);
  switch (op) {
    case '1':
      printf("time is out\n");
      break;
    case '2':
      printf("recv data\n");
      break;
  }
}

void* Select::Test(void* arg) {
  Select* s = static_cast<Select*>(arg);
  s->MySelect();
}

#endif  // RYAN_DS_KV_RAFT_SELECT_H_