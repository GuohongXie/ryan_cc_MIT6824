#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <condition_variable>
//#include <any>
#include <thread>

#include "buttonrpc.hpp"


//可能造成的bug，考虑write多次写，每次写1024用while读进buf
// c_str()返回的是一个临时指针，值传递没事，但若涉及地址出错

//定义实际处理map任务的数组，存放map任务号
//(map任务大于总文件数时，多线程分配ID不一定分配到正好增序的任务号，如10个map任务，总共8个文件，可能就是0,1,2,4,5,7,8,9)
struct KeyValue {
  std::string key;
  std::string value;
};

using MapFunc = std::vector<KeyValue> (*)(KeyValue);
using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);


class Worker {
 public:
  //定义的两个函数指针用于动态加载动态库里的map和reduce函数
  using MapFunc = std::vector<KeyValue> (*)(KeyValue);
  using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);
  static MapFunc map_func;
  static ReduceFunc reduce_func;
  static constexpr int MAX_REDUCE_NUM = 15;

  Worker(int disabled_map_id,     //用于人为让特定map任务超时的Id
         int disabled_reduce_id,  //用于人为让特定reduce任务超时的Id
         int map_task_num,
         int reduce_task_num) 
      : disabled_map_id_(disabled_map_id),
        disabled_reduce_id_(disabled_reduce_id),
        map_task_num_(map_task_num),
        reduce_task_num_(reduce_task_num) {}
  Worker() 
      : disabled_map_id_(0),
        disabled_reduce_id_(0),
        map_task_num_(0),
        reduce_task_num_(0) {}
 
  ~Worker() {}

  void* MapWorker(void* arg);
  void* ReduceWorker(void* arg);
  void RemoveTmpFiles();
  void RemoveOutputFiles();

  void set_map_task_num(int num) { map_task_num_ = num; }
  void set_reduce_task_num(int num) { reduce_task_num_ = num; }
  int map_task_num() const { return map_task_num_; }
  int reduce_task_num() const { return reduce_task_num_; }

 private:
  int Ihash(std::string str);  //对每个字符串求hash找到其对应要分配的reduce线程
  void MyWrite(int fd, std::vector<std::string>& str);  //用于最后写入磁盘的函数，输出最终结果
  KeyValue GetContent(const std::string& file_name);
  std::vector<std::string> GetAllfile(std::string path, int op); //获取对应reduce编号的所有中间文件
  std::vector<KeyValue> MyShuffle(int reduce_task_num);  // vector中每个元素的形式为"abc 11111";
  void WriteKV(int fd, const KeyValue& kv);
  void WriteInDisk(const std::vector<KeyValue>& kvs, int map_task_index);
  std::vector<std::string> Split(std::string text, char op); //以char类型的op为分割拆分字符串
  std::string Split(std::string text); //以char类型的op为分割拆分字符串

  std::mutex mutex_;
  std::condition_variable cond_;
  int disabled_map_id_;     //用于人为让特定map任务超时的Id
  int disabled_reduce_id_;  //用于人为让特定reduce任务超时的Id
  int map_task_num_;  //由coordinator分配，通过main函数的rpc从coordinator获取
  int reduce_task_num_;  //由coordinator分配，通过main函数的rpc从coordinator获取
  static int map_id_;  //给每个线程分配任务的id，用于写中间文件的命名
};
  