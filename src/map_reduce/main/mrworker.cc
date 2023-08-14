#include "map_reduce/mr/worker.cc"
#include <string>

//下面这俩行写在worker.h文件里面了，可移植性更好
// using MapFunc = std::vector<KeyValue> (*)(KeyValue);
// using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);

//用于加载的动态库的路径
const std::string LIB_CACULATE_PATH_STRING = "../mrapps/libmr_word_count.so";  
//RPC服务端口和IP
const int RPC_COORDINATOR_SERVER_PORT = 5555;
const std::string RPC_COORDINATOR_SERVER_IP = "127.0.0.1";


int main() {
  //整个程序中只有一个worker实体, 在多线程中共享
  Worker worker(RPC_COORDINATOR_SERVER_IP, 
                RPC_COORDINATOR_SERVER_PORT,
                0, 
                0, 
                0,
                0,
                0);

  //运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
  void* handle = dlopen(LIB_CACULATE_PATH_STRING.c_str(), RTLD_LAZY);
  if (!handle) {
    std::cerr << "Cannot open library: " << dlerror() << '\n';
    exit(-1);
  }
  worker.map_func = (MapFunc)dlsym(handle, "MapFunc");
  if (!worker.map_func) {
    std::cerr << "Cannot load symbol 'hello': " << dlerror() << '\n';
    dlclose(handle);
    exit(-1);
  }
  worker.reduce_func = (ReduceFunc)dlsym(handle, "ReduceFunc");
  if (!worker.reduce_func) {
    std::cerr << "Cannot load symbol 'hello': " << dlerror() << '\n';
    dlclose(handle);
    exit(-1);
  }

  //作为RPC请求端
  buttonrpc worker_client;
  worker_client.as_client(RPC_COORDINATOR_SERVER_IP,
                          RPC_COORDINATOR_SERVER_PORT);
  worker_client.set_timeout(5000);

  //获取rpc_coordinator_server提供的map_num和reduce_num并写入worker的成员变量
  int map_task_num = worker_client.call<int>("map_num").val();
  int reduce_task_num = worker_client.call<int>("reduce_num").val();
  worker.set_map_task_num(map_task_num);
  worker.set_reduce_task_num(reduce_task_num);
  worker.RemoveTmpFiles();     //若有，则清理上次输出的中间文件
  worker.RemoveOutputFiles();  //清理上次输出的最终文件

  // 创建多个 map 及 reduce 的 worker 线程
  // TODO:易错，不能直接创建线程，std::thread对象不可复制
  //以下是错误用法:
  // std::vector<std::thread> map_threads(worker.map_task_num());
  // std::vector<std::thread> reduce_threads(worker.reduce_task_num());

  // 创建map_task_num个map线程
  for (int i = 0; i < map_task_num; i++) {
    std::thread(&Worker::MapWorker, &worker).detach();
  }
  // TODO:这里为了在类外用类的成员mutex_和cond_，把他们设置成了public，破坏了数据封装的原则
  //以上已解决，在类封装cond_的相关操作，暴露给外部的只有WaitForMapDone()接口
  // TODO: condition_variable的使用需要结合while循环，否则会出现虚假唤醒
  //std::unique_lock<std::mutex> lock1(Worker::mutex_);
  //while (!worker_client.call<bool>("IsMapDone").val()) {
  //  Worker::cond_.wait(lock1);
  //}
  //lock1.unlock();
  worker.WaitForMapDone();

  // 创建map_task_num个map线程
  for (int i = 0; i < reduce_task_num; i++) {
    std::thread(&Worker::ReduceWorker, &worker).detach();
  }

  // 循环检查任务是否完成
  while (!worker_client.call<bool>("IsAllMapAndReduceDone").val()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // 任务完成后清理中间文件，关闭打开的动态库，释放资源
  worker.RemoveTmpFiles();
  ::dlclose(handle);
}