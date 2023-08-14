#ifndef RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_
#define RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_

#include <cstdio>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <unistd.h>  // ::sleep()

class Coordinator {
 public:
  void WaitMapTask();     //回收map的定时线程
  void WaitReduceTask();  //回收reduce的定时线程

  //虽然对于小对象 char，直接传值更快，因为会使用寄存器传参
  //但是这里为了能接受如字面值这样的常量，还是传引用
  static void WaitTime(const char& map_or_reduce_tag);  //用于定时的线程

  //带缺省值的有参构造，也可通过命令行传参指定，我偷懒少打两个数字直接放构造函数里
  explicit Coordinator(int map_num = 8, int reduce_num = 8);  

  void GetAllFile(int argc, char* argv[]);  //从argv[]中获取待处理的文件名

  int map_num() const { return map_num_; }
  int reduce_num() const { return reduce_num_; }

  std::string AssignMapTask();            //分配map任务的函数，RPC
  int AssignReduceTask();                 //分配reduce任务的函数，RPC
  void SetAMapTaskFinished(const std::string& file_name); //设置特定map任务完成的函数，RPC
  bool IsMapDone();                       //检验所有map任务是否完成，RPC
  void SetAReduceTaskFinished(int task_index);     //设置特定reduce任务完成的函数，RPC

  void WaitMap(const std::string& file_name);
  void WaitReduce(int reduce_idx);
  bool IsAllMapAndReduceDone(); //reduce完成即全部完成 RPC

 private:
  static constexpr int MAP_TASK_TIMEOUT = 3;
  static constexpr int REDUCE_TASK_TIMEOUT = 5;

  std::mutex mutex_;
  int input_file_num_;  //从命令行读取到的文件总数
  int map_num_;
  int reduce_num_;
  std::list<std::string> input_file_name_list_{};  //所有map任务的工作队列
  std::unordered_map<std::string, int> finished_map_task_{};  //input_file_name : map_index
  std::unordered_map<int, int> finished_reduce_task_{};  //存放所有完成的reduce任务对应的reduce编号
  std::vector<int> reduce_index_{};  //所有reduce任务的工作队列

  int curr_map_index_{0};    //当前处理第几个map任务
  int curr_reduce_index_{0};  //当前处理第几个reduce任务

  //正在处理的map任务，分配出去就加到这个队列，用于判断超时处理重发
  std::vector<std::string> running_map_work_;  
  //正在处理的reduce任务，分配出去就加到这个队列，用于判断超时处理重发
  std::vector<int> running_reduce_work_;  
};

#endif  // RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_