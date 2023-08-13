#ifndef RYAN_DS_MAP_REDUCE_MR_WORKER_H_
#define RYAN_DS_MAP_REDUCE_MR_WORKER_H_
// TODO:差点忘了头文件保护，写任何头文件的第一步就是写头文件保护

#include <condition_variable>
#include <cstdio>
#include <iostream>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
//#include <any>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <thread>

#include <dirent.h>  //struct dirent* entry;
#include <dlfcn.h>   //dlopen, dlclose
#include <fcntl.h>   // ::open()
#include <unistd.h>  //write, access, read, sleep


#include "buttonrpc/buttonrpc.hpp"

//可能造成的bug，考虑write多次写，每次写1024用while读进buf
// c_str()返回的是一个临时指针，值传递没事，但若涉及地址出错

//定义实际处理map任务的数组，存放map任务号
//(map任务大于总文件数时，多线程分配ID不一定分配到正好增序的任务号，如10个map任务，总共8个文件，可能就是0,1,2,4,5,7,8,9)
struct KeyValue {
  std::string key;
  std::string value;
  KeyValue(std::string k, std::string v) : key(std::move(k)), value(std::move(v)) {}
};

using MapFunc = std::vector<KeyValue> (*)(KeyValue);
using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);

class Worker {
 public:
  //定义的两个函数指针用于动态加载动态库里的map和reduce函数
  using MapFunc = std::vector<KeyValue> (*)(KeyValue);
  using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);
  MapFunc map_func;
  ReduceFunc reduce_func;
  static constexpr int MAX_REDUCE_NUM = 15;
  // mutex_和cond_要在类外用到, 所以我放在public，暂时没想到更好的设计
  //所有的map线程和reduce线程共享一个互斥量，所以锁的粒度要尽可能小
  static std::mutex mutex_;
  static std::condition_variable cond_;

  Worker(const std::string& rpc_coordinator_server_ip, 
         int rpc_coordinator_server_port,
         int disabled_map_id,     //用于人为让特定map任务超时的Id
         int disabled_reduce_id,  //用于人为让特定reduce任务超时的Id
         int map_task_num, int reduce_task_num)
      : kRpcCoordinatorServerIp_(rpc_coordinator_server_ip),
        kRpcCoordinatorServerPort_(rpc_coordinator_server_port),
        disabled_map_id_(disabled_map_id),
        disabled_reduce_id_(disabled_reduce_id),
        map_task_num_(map_task_num),
        reduce_task_num_(reduce_task_num) {}
  Worker()
      : kRpcCoordinatorServerIp_("127.0.0.1"),
        kRpcCoordinatorServerPort_(5555),
        disabled_map_id_(0),
        disabled_reduce_id_(0),
        map_task_num_(0),
        reduce_task_num_(0) {}

  ~Worker() = default;

  // noncopyable
  Worker(const Worker&) = delete;
  Worker operator=(const Worker&) = delete;

  void MapWorker();
  void ReduceWorker();
  // void* ReduceWorker(void* arg);
  //原始形式的ReduceWorker是non std::thread风格的多线程约定
  // in that case, when you need to create a new thread, you write as:
  // std::thread(&Worker::ReduceWorker, &worker, nullptr)
  // while in std::thread style
  // std::thread(&Worker::ReduceWorker, &worker)
  void RemoveTmpFiles() const ;
  static void RemoveOutputFiles();

  void set_map_task_num(int num) { map_task_num_ = num; }
  void set_reduce_task_num(int num) { reduce_task_num_ = num; }
  int map_task_num() const { return map_task_num_; }
  int reduce_task_num() const { return reduce_task_num_; }

 private:
  //对每个字符串求hash找到其对应要分配的reduce线程
  int Ihash(const std::string& str) const;  

  //用于最后写入磁盘的函数，输出最终结果
  void MyWrite(int fd, std::vector<std::string>& str);  

  KeyValue GetContent(const std::string& file_name_str);

  //获取对应reduce编号的所有中间文件
  std::vector<std::string> GetAllfile(const std::string& path, int op);  

  // vector中每个元素的形式为"abc 11111";
  std::vector<KeyValue> MyShuffle(int reduce_task_num);  

  void WriteKV(int fd, const KeyValue& kv);
  void WriteInDisk(const std::vector<KeyValue>& kvs, int map_task_index);

  //以char类型的op为分割拆分字符串
  std::vector<std::string> Split(const std::string& text, char op);  
  //以char类型的op为分割拆分字符串
  std::string Split(const std::string& text);  

  int disabled_map_id_;     //用于人为让特定map任务超时的Id
  int disabled_reduce_id_;  //用于人为让特定reduce任务超时的Id
  int map_task_num_;  //由coordinator分配，通过main函数的rpc从coordinator获取
  int reduce_task_num_;  //由coordinator分配，通过main函数的rpc从coordinator获取
  static int map_id_;  //给每个线程分配任务的id，用于写中间文件的命名
  // const成员函数在构造函数的初始化列表中进行初始化，不能在构造函数的函数体初始化，也不能类内初始化
  // TODO: c++11开始const成员函数可以类内初始化
  //但是需要注意的是一旦在构造函数初始化列表中进行const成员变量的初始化，就不能同时有类内初始化
  //不然编译期错误
  const std::string kRpcCoordinatorServerIp_;  //因为只有一个coordinator
  const int
      kRpcCoordinatorServerPort_;  //所以MapWorker和ReduceWorker共用一个server,故只需指定一个ip和port
};

#endif  // RYAN_DS_MAP_REDUCE_MR_WORKER_H_