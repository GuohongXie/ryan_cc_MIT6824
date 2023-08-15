#include "map_reduce/mr/coordinator.h"

#include <cassert>
#include <mutex>

Coordinator::Coordinator(int map_worker_num, int reduce_worker_num)
    : map_worker_num_(map_worker_num),
      reduce_worker_num_(reduce_worker_num),
      input_file_num_(0) {
  if (map_worker_num_ <= 0 || reduce_worker_num_ <= 0) {
    throw std::exception();
  }
  for (int i = 0; i < reduce_worker_num; i++) {
    reduce_task_queue_.push(i);
  }
}

/// @brief 从argv[]中获取待处理的文件名, 存入input_file_name_list_
/// for initialization
/// "mr_coordinator input_file_name_1 input_file_name_2 ..."
/// "mrcoordinator pg-*.txt"
void Coordinator::GetAllFile(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    map_task_queue_.push(argv[i]);
  }
  input_file_num_ = argc - 1;
  assert(input_file_num_ == map_task_queue_.size());
}

///////////// map //////////////

/// @brief 分配map任务的函数，RPC, called by MapWorker
/// @return std::string 返回待map的文件名
std::string Coordinator::AssignAMapTask() {
  if (IsMapDone()) return "empty";
  if (!map_task_queue_.empty()) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::string map_task =
        map_task_queue_.front();  //从工作队列取出一个待map的文件名
    map_task_queue_.pop();
    lock.unlock();
    //开启一个计时线程，若超时后仍未完成map任务，将该任务重新加入工作队列
    std::thread t([this, map_task]() { this->WaitMapTask(map_task); });
    t.detach();
    return map_task;
  }
  return "empty";
}

// RPC, called by MapWorker
// when map done, MapWorker will notify main thread of mrworker
// and then return, so the MapWorker thread will be terminated
bool Coordinator::IsMapDone() {
  std::lock_guard<std::mutex> lock(mutex_);
  // safe:先对return的表达式求值，再对lock进行解锁，再离开作用域
  return finished_map_task_.size() == input_file_num_;
}

void Coordinator::WaitMapTask(const std::string& map_task) {
  //等待一个超时时间MAP_TASK_TIMEOUT
  std::thread tid([]() { WaitTime('m'); });
  tid.join();  // join方式回收实现超时后解除阻塞

  std::lock_guard<std::mutex> lock(mutex_);
  //若超时后在对应的hashmap中没有该map任务完成的记录，重新将该任务加入工作队列
  if (finished_map_task_.count(map_task) == 0) {
    printf("map task : %s is timeout\n", map_task.c_str());
    map_task_queue_.push(map_task);
    return;
  }
}

//分map任务还是reduce任务进行不同时间计时的计时线程
void Coordinator::WaitTime(const char& map_or_reduce_tag) {
  if (map_or_reduce_tag == 'm') {
    ::sleep(MAP_TASK_TIMEOUT);
  } else if (map_or_reduce_tag == 'r') {
    ::sleep(REDUCE_TASK_TIMEOUT);
  }
}

///////////// reduce //////////////
/// @brief 分配reduce任务的函数，RPC, called by ReduceWorker
/// @return int 返回从工作队列中取出的任务名
int Coordinator::AssignAReduceTask() {
  if (IsAllMapAndReduceDone()) {
    printf("all map and reduce task is done\n");
    return -1;
  }
  if (!reduce_task_queue_.empty()) {
    std::unique_lock<std::mutex> lock(mutex_);
    int reduce_task = reduce_task_queue_.front();  //取出reduce编号
    reduce_task_queue_.pop();
    lock.unlock();
    //开启一个计时线程，若超时后仍未完成reduce任务，将该任务重新加入工作队列
    std::thread t([this, reduce_task]() { this->WaitReduceTask(reduce_task); });
    t.detach();
    return reduce_task;
  }
  return -1;
}

bool Coordinator::IsAllMapAndReduceDone() {
  std::lock_guard<std::mutex> lock(mutex_);
  return finished_reduce_task_.size() == reduce_worker_num_;
}

void Coordinator::WaitReduceTask(int reduce_task) {
  std::thread t([]() { WaitTime('r'); });
  t.join();
  std::lock_guard<std::mutex> lock(mutex_);
  //若超时后在对应的hashmap中没有该reduce任务完成的记录，将该任务重新加入工作队列
  if (finished_reduce_task_.find(reduce_task) == finished_reduce_task_.end()) {
    reduce_task_queue_.push(reduce_task);
    printf("reduce task : %d is timeout\n", reduce_task);
    return;
  }
}

///////////// set task finished //////////////

//通过worker的RPC调用修改map任务的完成状态
//需要说明的是此处无论传值还是传引用都可以
//因为通过RPC调用，参数会被序列化和反序列化，传递的是参数的副本
//而不是另一个进程中的引用
//这里为了语义的清晰，使用了传引用的方式
void Coordinator::SetAMapTaskFinished(const std::string& map_task) {
  std::lock_guard<std::mutex> lock(mutex_);
  finished_map_task_.insert(map_task);
  printf("map task : %s is finished\n", map_task.c_str());
}

//通过worker的RPC调用修改reduce任务的完成状态
void Coordinator::SetAReduceTaskFinished(int reduce_task) {
  std::lock_guard<std::mutex> lock(mutex_);
  finished_reduce_task_.insert(reduce_task);
  printf("reduce task : %d is finished\n", reduce_task);
}
