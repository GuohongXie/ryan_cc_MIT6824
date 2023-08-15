#include <string>

#include "map_reduce/mr/worker.cc"

//下面这俩行写在worker.h文件里面了，可移植性更好
// using MapFunc = std::vector<KeyValue> (*)(KeyValue);
// using ReduceFunc = std::vector<std::string> (*)(std::vector<KeyValue>, int);

//用于加载的动态库的路径
const std::string LIB_CACULATE_PATH_STRING = "../mrapps/libmr_word_count.so";
// RPC服务端口和IP
const int RPC_COORDINATOR_SERVER_PORT = 5555;
const std::string RPC_COORDINATOR_SERVER_IP = "127.0.0.1";

int main() {
  //整个程序中只有一个worker实体, 在多线程中共享
  //构造worker的初始值怎么设置无所谓，因为后面会通过rpc获取真实的map_worker_num和reduce_worker_num
  Worker worker(RPC_COORDINATOR_SERVER_IP, RPC_COORDINATOR_SERVER_PORT,
                0,   // disabled_map_id
                0,   // disabled_reduce_id
                0);  // map_id

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

  //获取rpc_coordinator_server提供的map_worker_num和reduce_worker_num并写入worker的成员变量
  int map_worker_num = worker_client.call<int>("map_worker_num").val();
  int reduce_worker_num = worker_client.call<int>("reduce_worker_num").val();
  worker.set_map_worker_num(map_worker_num);
  worker.set_reduce_worker_num(reduce_worker_num);
  worker.RemoveTmpFiles();     //若有，则清理上次输出的中间文件
  worker.RemoveOutputFiles();  //清理上次输出的最终文件

  //////// 创建多个 map 及 reduce 的 worker 线程 ////////

  // 创建map_worker_num个map线程
  for (int i = 0; i < map_worker_num; i++) {
    std::thread(&Worker::MapWorker, &worker).detach();
  }
  // 等待所有map线程完成
  worker.WaitForMapDone();

  // 创建reduce_worker_num个map线程
  for (int i = 0; i < reduce_worker_num; i++) {
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