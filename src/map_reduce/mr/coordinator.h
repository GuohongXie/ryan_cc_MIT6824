#ifndef RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_
#define RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_

#include <unistd.h>  // ::sleep()

//#include <iostream>
#include <cstdio>
#include <string>
#include <vector>
#include <unordered_map>
#include <list>
#include <mutex>
#include <thread>
//#include <condition_variable>
//#include <any>
//#include <chrono>

class Coordinator {
 public:
  // std::any is a new feature in c++17
  static void* WaitMapTask(void* arg);     //回收map的定时线程
  static void* WaitReduceTask(void* arg);  //回收reduce的定时线程
  static void* WaitTime(void* arg);        //用于定时的线程
  Coordinator(
      int map_num = 8,
      int reduce_num = 8);  //带缺省值的有参构造，也可通过命令行传参指定，我偷懒少打两个数字直接放构造函数里
  void GetAllFile(int argc, char* argv[]);  //从argv[]中获取待处理的文件名
  int map_num() { return map_num_; }
  int reduce_num() { return reduce_num_; }
  std::string AssignTask();               //分配map任务的函数，RPC
  int AssignReduceTask();            //分配reduce任务的函数，RPC
  void SetMapStat(std::string file_name);  //设置特定map任务完成的函数，RPC
  bool IsMapDone();                  //检验所有map任务是否完成，RPC
  void SetReduceStat(int task_index);  //设置特定reduce任务完成的函数，RPC
  void WaitMap(std::string file_name);
  void WaitReduce(int reduce_idx);
  bool IsAllMapAndReduceDone();  //判断reduce任务是否已经完成
  bool GetFinalStat() {  //所有任务是否完成，实际上reduce完成就完成了，有点小重复
    return done_;
  }

 private:
  static constexpr int MAP_TASK_TIMEOUT = 3;
  static constexpr int REDUCE_TASK_TIMEOUT = 5;
  std::mutex mutex_;
  //std::condition_variable cv_;
  bool done_;
  std::list<std::string> input_file_name_list_;  //所有map任务的工作队列
  int input_file_num_;  //从命令行读取到的文件总数
  int map_num_;
  int reduce_num_;
  std::unordered_map<std::string, int> finished_map_task_;  //存放所有完成的map任务对应的文件名
  std::unordered_map<int, int> finished_reduce_task_;  //存放所有完成的reduce任务对应的reduce编号
  std::vector<int> reduce_index_;  //所有reduce任务的工作队列
  std::vector<std::string> running_map_work_;  //正在处理的map任务，分配出去就加到这个队列，用于判断超时处理重发
  int curr_map_index_;  //当前处理第几个map任务
  int curr_reduce_index_;  //当前处理第几个reduce任务
  std::vector<int> running_reduce_work_;  //正在处理的reduce任务，分配出去就加到这个队列，用于判断超时处理重发
};


#endif //RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_