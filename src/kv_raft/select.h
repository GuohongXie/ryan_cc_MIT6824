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

class Select {
 public:
  explicit Select(const std::string& fifo_name);
  int timeout{};
  std::string fifo_name;
  char op{};
  std::mutex mutex;
  std::condition_variable cond;
  void MySelect();
  static void WaitTime();
  void Work();
  void SetTimeOut();
  void Send() const;
  void Test();
};

Select::Select(const std::string& fifo_name) {
  this->fifo_name = fifo_name;
  int ret = ::mkfifo(fifo_name.c_str(), 0664);
  std::thread(&Select::Send, this).detach();
  std::thread(&Select::Test, this).detach();
}

void Select::Send() const {
  int fd = ::open(fifo_name.c_str(), O_WRONLY);
  char buf[] = "12345";
  ::sleep(1);
  ::write(fd, buf, strlen(buf) + 1);
}

void Select::WaitTime() { ::sleep(2); }

void Select::SetTimeOut() {
  std::thread t(&Select::WaitTime);
  t.join();
  std::unique_lock<std::mutex> lock(mutex);
  op = '1';
  cond.notify_one();
}

void Select::Work() {
  char buf[100];
  int fd = ::open(fifo_name.c_str(), O_RDONLY);
  ::read(fd, buf, sizeof(buf));
  std::unique_lock<std::mutex> lock(mutex);
  op = '2';
  cond.notify_one();
}

void Select::MySelect() {
  std::thread(&Select::SetTimeOut, this).detach();
  std::thread(&Select::Work, this).detach();
  std::unique_lock<std::mutex> lock(mutex);
  cond.wait(lock);
  switch (op) {
    case '1':
      printf("time is out\n");
      break;
    case '2':
      printf("recv data\n");
      break;
  }
}

void Select::Test() {
  MySelect();
}

#endif  // RYAN_DS_KV_RAFT_SELECT_H_