#ifndef RYAN_DS_KV_RAFT_SEMAPHORE_H_
#define RYAN_DS_KV_RAFT_SEMAPHORE_H_

#include <chrono>
#include <condition_variable>
#include <mutex>

class Semaphore {
 public:
  explicit Semaphore(int count = 0) : count_(count) {}

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

  bool TimedWait(unsigned long milliseconds) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (condition_.wait_for(lock, std::chrono::milliseconds(milliseconds),
                            [this]() { return count_ > 0; })) {
      count_--;
      return true;
    } else {
      return false;
    }
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  int count_;
};

#endif  // RYAN_DS_KV_RAFT_SEMAPHORE_H_