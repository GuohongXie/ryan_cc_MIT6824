#ifndef RYAN_DS_KV_RAFT_SEMAPHORE_H_
#define RYAN_DS_KV_RAFT_SEMAPHORE_H_

#include <condition_variable>
#include <mutex>

class Semaphore {
 public:
  Semaphore(int count = 0) : count_(count) {}

  inline void Init(int count = 0) { count_ = count; }

  inline bool Post() {
    std::unique_lock<std::mutex> lock(mutex_);
    count_++;
    condition_.notify_one();
    return true;
  }

  inline bool Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (count_ == 0) {
      condition_.wait(lock);
    }
    count_--;
    return true;
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  int count_;
};

#endif  // RYAN_DS_KV_RAFT_SEMAPHORE_H_