#ifndef RYAN_DS_MAP_REDUCE_MR_WORKER_H_
#define RYAN_DS_MAP_REDUCE_MR_WORKER_H_

#include <dirent.h>  //struct dirent* entry;
#include <dlfcn.h>   //dlopen, dlclose
#include <fcntl.h>   // ::open()
#include <unistd.h>  //write, access, read, sleep

#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <list>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "buttonrpc/buttonrpc.hpp"

//定义实际处理map任务的数组，存放map任务号
//(map任务大于总文件数时，多线程分配ID不一定分配到正好增序的任务号，如10个map任务，总共8个文件，可能就是0,1,2,4,5,7,8,9)
struct KeyValue {
  std::string key;
  std::string value;
  KeyValue(std::string k, std::string v)
      : key(std::move(k)), value(std::move(v)) {}
};

using MapFunc = std::vector<KeyValue> (*)(KeyValue);
using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);

class Worker {  // noncopyable
 public:
  //定义的两个函数指针用于动态加载动态库里的map和reduce函数
  using MapFunc = std::vector<KeyValue> (*)(KeyValue);
  using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);
  MapFunc map_func{};
  ReduceFunc reduce_func{};
  static constexpr int MAX_REDUCE_NUM = 15;  //编译器常量

  Worker(const std::string& rpc_coordinator_server_ip,
         int rpc_coordinator_server_port,
         int disabled_map_id,     //用于人为让特定map任务超时的Id
         int disabled_reduce_id,  //用于人为让特定reduce任务超时的Id
         int map_id_)
      : kRpcCoordinatorServerIp_(rpc_coordinator_server_ip),
        kRpcCoordinatorServerPort_(rpc_coordinator_server_port),
        disabled_map_id_(disabled_map_id),
        disabled_reduce_id_(disabled_reduce_id),
        map_id_(map_id_) {}
  Worker()
      : kRpcCoordinatorServerIp_("127.0.0.1"),
        kRpcCoordinatorServerPort_(5555),
        disabled_map_id_(0),
        disabled_reduce_id_(0),
        map_id_(0) {}

  ~Worker() = default;

  // noncopyable
  Worker(const Worker&) = delete;
  Worker operator=(const Worker&) = delete;

  void MapWorker();
  void ReduceWorker();

  void WaitForMapDone();

  void RemoveTmpFiles() const;
  static void RemoveOutputFiles();

  void set_map_worker_num(int num) { map_worker_num_ = num; }
  void set_reduce_worker_num(int num) { reduce_worker_num_ = num; }
  int map_worker_num() const { return map_worker_num_; }
  int reduce_worker_num() const { return reduce_worker_num_; }

 private:
  void NotifyMapDone() { cond_.notify_all(); }
  //对每个字符串求hash找到其对应要分配的reduce线程
  int Ihash(const std::string& str) const;

  //用于最后写入磁盘的函数，输出最终结果
  static void MyWrite(int fd, std::vector<std::string>& str);

  static KeyValue GetContent(const std::string& file_name_str);

  //获取对应reduce编号的所有中间文件
  static std::vector<std::string> GetAllfile(const std::string& directory_str,
                                             int reduce_task_index);

  // vector中每个元素的形式为"abc 11111";
  static std::vector<KeyValue> MyShuffle(int reduce_task_index);

  static void WriteKV(int fd, const KeyValue& kv);
  void WriteInDisk(const std::vector<KeyValue>& kvs, int map_worker_index);

  //"word,1 word,1 word,1 ..." -> ["word,1", "word,1", "word,1", ...]
  static std::vector<std::string> Split(const std::string& text,
                                        char seporator);
  //"word, 1" -> "word"
  static std::string Split(const std::string& text);

  int disabled_map_id_;     //用于人为让特定map任务超时的Id
  int disabled_reduce_id_;  //用于人为让特定reduce任务超时的Id
  int map_worker_num_{
      0};  //由coordinator分配，通过main函数的rpc从coordinator获取
  int reduce_worker_num_{
      0};  //由coordinator分配，通过main函数的rpc从coordinator获取
  int map_id_;  //给每个线程分配任务的id，用于写中间文件的命名
  bool is_map_done_ = false;  //用于判断map任务是否完成
  // const成员函数在构造函数的初始化列表中进行初始化，不能在构造函数的函数体初始化，也不能类内初始化
  // TODO: c++11开始const成员函数可以类内初始化
  //但是需要注意的是一旦在构造函数初始化列表中进行const成员变量的初始化，就不能同时有类内初始化
  //不然编译期错误
  const std::string
      kRpcCoordinatorServerIp_;  // MapWorker()和ReduceWorker()共用rpc server
  const int kRpcCoordinatorServerPort_;  //(coordinator), 故只需指定一个ip和port

  // 所有的map线程和reduce线程共享一个互斥量，所以锁的粒度要尽可能小
  std::mutex mutex_;
  std::condition_variable cond_;
};

#endif  // RYAN_DS_MAP_REDUCE_MR_WORKER_H_