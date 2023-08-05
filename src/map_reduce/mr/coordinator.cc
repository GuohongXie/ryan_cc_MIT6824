#include "map_reduce/mr/coordinator.h"

Coordinator::Coordinator(int map_num, int reduce_num)
    : done_(false), map_num_(map_num), reduce_num_(reduce_num) {
  input_file_name_list_.clear();
  finished_map_task_.clear();
  finished_reduce_task_.clear();
  running_map_work_.clear();
  running_reduce_work_.clear();
  curr_map_index_ = 0;
  curr_reduce_index_ = 0;
  if (map_num_ <= 0 || reduce_num_ <= 0) {
    throw std::exception();
  }
  for (int i = 0; i < reduce_num; i++) {
    reduce_index_.emplace_back(i);
  }
}

/// @brief get all the source file names(std::string) into input_file_name_list_
/// @param argc is the number of command line arguments
/// @param argv is the value of command line arguments
void Coordinator::GetAllFile(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    input_file_name_list_.emplace_back(argv[i]);
  }
  input_file_num_ = argc - 1;
}

// map的worker只需要拿到对应的文件名就可以进行map
std::string Coordinator::AssignTask() {
  if (IsMapDone()) return "empty";
  if (!input_file_name_list_.empty()) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::string task =
        input_file_name_list_.back();  //从工作队列取出一个待map的文件名
    input_file_name_list_.pop_back();
    lock.unlock();
    WaitMap(
        task);  //调用waitMap将取出的任务加入正在运行的map任务队列并等待计时线程
    return task;
  }
  return "empty";
}

void* Coordinator::WaitMapTask(void* arg) {
  auto map = static_cast<Coordinator*>(arg);
  std::thread tid([&]() {
    char op = 'm';
    WaitTime(&op);
  });
  tid.join();  // join方式回收实现超时后解除阻塞
  std::unique_lock<std::mutex> lock(map->mutex_);
  //若超时后在对应的hashmap中没有该map任务完成的记录，重新将该任务加入工作队列
  if (!map->finished_map_task_.count(
          map->running_map_work_[map->curr_map_index_])) {
    printf("file_name : %s is timeout\n",
           map->running_map_work_[map->curr_map_index_].c_str());
    // char text[map->running_map_work_[map->curr_map_index_].size() + 1];
    // strcpy(text, map->running_map_work_[map->curr_map_index_].c_str());
    // printf("text is %s\n", text);
    // 打印正常的，该线程结束后text就变成空字符串了
    const std::string& text =
        map->running_map_work_
            [map->curr_map_index_];  //这钟方式加入list后不会变成空字符串
    map->input_file_name_list_.push_back(text);
    map->curr_map_index_++;
    lock.unlock();
    return nullptr;
  }
  printf("file_name : %s is finished at idx : %d\n",
         map->running_map_work_[map->curr_map_index_].c_str(),
         map->curr_map_index_);
  ++(map->curr_map_index_);
}

void Coordinator::WaitMap(std::string file_name) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_map_work_.push_back(
        file_name);  //将分配出去的map任务加入正在运行的工作队列
  }
  std::thread t(&Coordinator::WaitMapTask,
                this);  //创建一个用于回收计时线程及处理超时逻辑的线程
  t.detach();
}

//分map任务还是reduce任务进行不同时间计时的计时线程
void* Coordinator::WaitTime(void* arg) {
  char* op = static_cast<char*>(arg);
  if (*op == 'm') {
    ::sleep(MAP_TASK_TIMEOUT);
  } else if (*op == 'r') {
    ::sleep(REDUCE_TASK_TIMEOUT);
  }
}

void* Coordinator::WaitReduceTask(void* arg) {
  auto reduce = static_cast<Coordinator*>(arg);
  void* status;
  std::thread t([&]() {
    char op = 'r';
    WaitTime(&op);
  });
  t.join();
  std::unique_lock<std::mutex> lock(reduce->mutex_);
  //若超时后在对应的hashmap中没有该reduce任务完成的记录，将该任务重新加入工作队列
  if (!reduce->finished_reduce_task_.count(
          reduce->running_reduce_work_[reduce->curr_reduce_index_])) {
    for (const auto& str : reduce->input_file_name_list_) {
      printf(" before insert %s\n", str.c_str());
    }
    reduce->reduce_index_.emplace_back(
        reduce->running_reduce_work_[reduce->curr_reduce_index_]);
    printf("%d reduce is timeout\n",
           reduce->running_reduce_work_[reduce->curr_reduce_index_]);
    (reduce->curr_reduce_index_)++;
    for (const auto& str : reduce->input_file_name_list_) {
      printf(" after insert %s\n", str.c_str());
    }
    lock.unlock();
    return nullptr;
  }
  printf("%d reduce is completed\n",
         reduce->running_reduce_work_[reduce->curr_reduce_index_]);
  reduce->curr_reduce_index_++;
}

void Coordinator::WaitReduce(int reduce_idx) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_reduce_work_.push_back(
        reduce_idx);  //将分配出去的reduce任务加入正在运行的工作队列
  }
  std::thread t(&Coordinator::WaitReduceTask,
                this);  //创建一个用于回收计时线程及处理超时逻辑的线程
  t.detach();
}

void Coordinator::SetMapStat(std::string file_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  finished_map_task_[file_name] = 1;  //通过worker的RPC调用修改map任务的完成状态
  // printf("map task : %s is finished, maphash is %p\n", file_name.c_str(),
  // &finished_map_task_);
}

bool Coordinator::IsMapDone() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (finished_map_task_.size() !=
      input_file_num_) {  //当统计map任务的hashmap大小达到文件数，map任务结束
    return false;
  }
  return true;
}

int Coordinator::AssignReduceTask() {
  if (IsAllMapAndReduceDone()) return -1;
  if (!reduce_index_.empty()) {
    std::unique_lock<std::mutex> lock(mutex_);
    int reduce_idx = reduce_index_.back();  //取出reduce编号
    reduce_index_.pop_back();
    lock.unlock();
    WaitReduce(
        reduce_idx);  //调用waitReduce将取出的任务加入正在运行的reduce任务队列并等待计时线程
    return reduce_idx;
  }
  return -1;
}

void Coordinator::SetReduceStat(int task_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  finished_reduce_task_[task_index] = 1;  //通过worker的RPC调用修改reduce任务的完成状态
  // printf(" reduce task%d is finished, reducehash is %p\n", task_index,
  // &finished_reduce_task_);
}

bool Coordinator::IsAllMapAndReduceDone() {
  std::lock_guard<std::mutex> lock(mutex_);
  return finished_reduce_task_.size() == reduce_num_;
}
