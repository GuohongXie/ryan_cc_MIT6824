#include "worker.h"
#include "buttonrpc.hpp"
#include <string>

using MapFunc = std::vector<KeyValue> (*)(KeyValue);
using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);

const std::string LIB_CACULATE_PATH_STRING = "./libmap_reduce.so";  //用于加载的动态库的路径
const int RPC_SERVER_PORT = 5555;
const std::string RPC_SERVER_IP = "127.0.0.1";


std::mutex mutex1;
std::condition_variable cond1;

int main() {
  Worker worker;

  //运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
  void* handle = dlopen(LIB_CACULATE_PATH_STRING.c_str(), RTLD_LAZY);
  if (!handle) {
    cerr << "Cannot open library: " << dlerror() << '\n';
    exit(-1);
  }
  Worker::map_func = (MapFunc)dlsym(handle, "MapFunc");
  if (!worker.map_func) {
    cerr << "Cannot load symbol 'hello': " << dlerror() << '\n';
    dlclose(handle);
    exit(-1);
  }
  Worker::reduce_func = (ReduceFunc)dlsym(handle, "ReduceFunc");
  if (!worker.reduce_func) {
    cerr << "Cannot load symbol 'hello': " << dlerror() << '\n';
    dlclose(handle);
    exit(-1);
  }

  //作为RPC请求端
  buttonrpc worker_client;
  worker_client.as_client(RPC_SERVER_IP.c_str(), RPC_SERVER_PORT);
  worker_client.set_timeout(5000);
  int map_task_num_tmp = worker_client.call<int>("map_num").val();
  int reduce_task_num_tmp = worker_client.call<int>("reduce_num").val();
  worker.set_map_task_num(map_task_num_tmp);
  worker.set_reduce_task_num(reduce_task_num_tmp);
  worker.RemoveTmpFiles();        //若有，则清理上次输出的中间文件
  worker.RemoveOutputFiles();  //清理上次输出的最终文件

  // 创建多个 map 及 reduce 的 worker 线程
  std::vector<std::thread> map_threads(worker.map_task_num());
  std::vector<std::thread> reduce_threads(worker.reduce_task_num());
  for (int i = 0; i < map_task_num_tmp; i++) {
    map_threads[i] = std::thread(&Worker::MapWorker, &worker);
    map_threads[i].detach();
  }
  std::unique_lock<std::mutex> lock1(mutex1);
  cond1.wait(lock1);
  lock1.unlock();
  for (int i = 0; i < reduce_task_num_tmp; i++) {
    reduce_threads[i] = std::thread(&Worker::ReduceWorker,  &worker);
    reduce_threads[i].detach();
  }

  // 循环检查任务是否完成
  while (!worker_client.call<bool>("Done").val()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // 任务完成后清理中间文件，关闭打开的动态库，释放资源
  worker.RemoveTmpFiles();
  ::dlclose(handle);
}