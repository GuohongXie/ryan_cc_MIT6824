#ifndef RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_
#define RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_

#include <unistd.h>  // ::sleep()

#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_set>

class Coordinator {
 public:
  //也可通过命令行传参指定，这里直接放构造函数里
  explicit Coordinator(int map_worker_num = 8, int reduce_worker_num = 8);
  // noncopyable
  Coordinator(const Coordinator&) = delete;
  Coordinator& operator=(const Coordinator&) = delete;

  ~Coordinator() = default;

  void GetAllFile(int argc, char* argv[]);  //从argv[]中获取待处理的文件名

  int map_worker_num() const { return map_worker_num_; }        // RPC
  int reduce_worker_num() const { return reduce_worker_num_; }  // RPC

  std::string AssignAMapTask();  //分配map任务的函数，RPC
  int AssignAReduceTask();       //分配reduce任务的函数，RPC
  void SetAMapTaskFinished(
      const std::string& map_task);  //设置特定map任务完成的函数，RPC
  bool IsMapDone();                  //检验所有map任务是否完成，RPC
  void SetAReduceTaskFinished(
      int reduce_task);  //设置特定reduce任务完成的函数，RPC

  bool IsAllMapAndReduceDone();  // reduce完成即全部完成 RPC

 private:
  void WaitMapTask(
      const std::string&
          map_task);  //判断map任务是否超时，超时则将任务重新加入队列
  void WaitReduceTask(
      int reduce_task);  // 判断reduce任务是否超时，超时则将任务重新加入队列

  static constexpr int MAP_TASK_TIMEOUT = 3;
  static constexpr int REDUCE_TASK_TIMEOUT = 5;

  std::mutex mutex_;  //map和reduce不会同时进行，可以共用一个mutex_(分时复用)
  std::condition_variable map_cond_;
  std::condition_variable reduce_cond_;
  int input_file_num_;  //从命令行读取到的文件总数

  //在coordinator中设置的map和reduce的worker线程数
  //当map_worker_num_比文件数多时，会有空闲的map线程
  // map_worker_num_和reduce_worker_num_建议设置为机器的核数
  int map_worker_num_;
  int reduce_worker_num_;

  std::queue<std::string>
      map_task_queue_{};  // map需要处理的文件名队列，即task队列
  std::queue<int> reduce_task_queue_{};                  // reduce任务队列
  std::unordered_set<std::string> finished_map_task_{};  //存放所有完成的map任务
  std::unordered_set<int> finished_reduce_task_{};  //存放所有完成的reduce任务
};

#endif  // RYAN_DS_MAP_REDUCE_MR_COORDINATOR_H_