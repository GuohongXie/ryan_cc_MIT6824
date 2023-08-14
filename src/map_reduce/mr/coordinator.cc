#include "map_reduce/mr/coordinator.h"

Coordinator::Coordinator(int map_num, int reduce_num)
    : map_num_(map_num), 
      reduce_num_(reduce_num),
      curr_map_index_(0),
      curr_reduce_index_(0),
      input_file_num_(0) {
  if (map_num_ <= 0 || reduce_num_ <= 0) {
    throw std::exception();
  }
  for (int i = 0; i < reduce_num; i++) {
    reduce_index_.emplace_back(i);
  }
}

/// @brief 从argv[]中获取待处理的文件名, 存入input_file_name_list_
/// "mr_coordinator input_file_name_1 input_file_name_2 ..."
/// "mrcoordinator pg-*.txt"
void Coordinator::GetAllFile(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    input_file_name_list_.emplace_back(argv[i]);
  }
  input_file_num_ = argc - 1;
}

/// @brief 分配map任务的函数，RPC
/// @return std::string 返回待map的文件名
std::string Coordinator::AssignMapTask() {
  if (IsMapDone()) return "empty";
  if (!input_file_name_list_.empty()) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::string task = input_file_name_list_.back();  //从工作队列取出一个待map的文件名
    input_file_name_list_.pop_back();
    lock.unlock();
    WaitMap(task);  //调用waitMap将取出的任务加入正在运行的map任务队列并等待计时线程
    return task;
  }
  return "empty";
}



void Coordinator::WaitMap(const std::string& file_name) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_map_work_.push_back(
        file_name);  //将分配出去的map任务加入正在运行的工作队列
  }
  std::thread t(&Coordinator::WaitMapTask,
                this);  //创建一个用于回收计时线程及处理超时逻辑的线程
  t.detach();
}

void Coordinator::WaitMapTask() {
  std::thread tid([]() { WaitTime('m'); });
  tid.join();  // join方式回收实现超时后解除阻塞
  std::unique_lock<std::mutex> lock(mutex_);
  //若超时后在对应的hashmap中没有该map任务完成的记录，重新将该任务加入工作队列
  if (!finished_map_task_.count(running_map_work_[curr_map_index_])) {
    printf("file_name : %s is timeout\n", running_map_work_[curr_map_index_].c_str());
    // char text[map->running_map_work_[map->curr_map_index_].size() + 1];
    // strcpy(text, map->running_map_work_[map->curr_map_index_].c_str());
    // printf("text is %s\n", text);
    // 打印正常的，该线程结束后text就变成空字符串了
    const std::string& text = running_map_work_[curr_map_index_];  //这钟方式加入list后不会变成空字符串
    input_file_name_list_.push_back(text);
    curr_map_index_++;
    lock.unlock();
    return;
  }
  printf("file_name : %s is finished at idx : %d\n", running_map_work_[curr_map_index_].c_str(), curr_map_index_);
  ++curr_map_index_;
}



//分map任务还是reduce任务进行不同时间计时的计时线程
void Coordinator::WaitTime(const char& map_or_reduce_tag) {
  if (map_or_reduce_tag == 'm') {
    ::sleep(MAP_TASK_TIMEOUT);
  } else if (map_or_reduce_tag == 'r') {
    ::sleep(REDUCE_TASK_TIMEOUT);
  }
}


void Coordinator::WaitReduceTask() {
  void* status;
  std::thread t([]() { WaitTime('r'); });
  t.join();
  std::unique_lock<std::mutex> lock(mutex_);
  //若超时后在对应的hashmap中没有该reduce任务完成的记录，将该任务重新加入工作队列
  if (!finished_reduce_task_.count(running_reduce_work_[curr_reduce_index_])) {
    for (const auto& str : input_file_name_list_) {
      printf(" before insert %s\n", str.c_str());
    }
    reduce_index_.emplace_back(running_reduce_work_[curr_reduce_index_]);
    printf("%d reduce is timeout\n", running_reduce_work_[curr_reduce_index_]);
    curr_reduce_index_++;
    for (const auto& str : input_file_name_list_) {
      printf(" after insert %s\n", str.c_str());
    }
    lock.unlock();
    return;
  }
  printf("%d reduce is completed\n", running_reduce_work_[curr_reduce_index_]);
  curr_reduce_index_++;
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

//这里传引用还是传值是一个值得思考的问题
//多线程环境中，传引用的前提是该变量不会被修改，否则会出现数据竞争
void Coordinator::SetAMapTaskFinished(const std::string& file_name) { 
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

void Coordinator::SetAReduceTaskFinished(int task_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  finished_reduce_task_[task_index] = 1;  //通过worker的RPC调用修改reduce任务的完成状态
  // printf(" reduce task%d is finished, reducehash is %p\n", task_index,
  // &finished_reduce_task_);
}

bool Coordinator::IsAllMapAndReduceDone() {
  std::lock_guard<std::mutex> lock(mutex_);
  return finished_reduce_task_.size() == reduce_num_;
}
