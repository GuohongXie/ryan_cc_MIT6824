#include "worker_old.cc"
#include "worker_old.h"
using namespace std;

#define LIB_CACULATE_PATH "./libmap_reduce.so"  //用于加载的动态库的路径

int main() {
  pthread_mutex_init(&map_mutex, NULL);
  pthread_cond_init(&cond, NULL);

  //运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
  void* handle = dlopen(LIB_CACULATE_PATH, RTLD_LAZY);
  if (!handle) {
    cerr << "Cannot open library: " << dlerror() << '\n';
    exit(-1);
  }
  mapF = (MapFunc)dlsym(handle, "MapFunc");
  if (!mapF) {
    cerr << "Cannot load symbol 'hello': " << dlerror() << '\n';
    dlclose(handle);
    exit(-1);
  }
  reduceF = (ReduceFunc)dlsym(handle, "ReduceFunc");
  if (!mapF) {
    cerr << "Cannot load symbol 'hello': " << dlerror() << '\n';
    dlclose(handle);
    exit(-1);
  }

  //作为RPC请求端
  buttonrpc work_client;
  work_client.as_client("127.0.0.1", 5555);
  work_client.set_timeout(5000);
  map_task_num = work_client.call<int>("map_num").val();
  reduce_task_num = work_client.call<int>("reduce_num").val();
  removeFiles();        //若有，则清理上次输出的中间文件
  removeOutputFiles();  //清理上次输出的最终文件

  //创建多个map及reduce的worker线程
  pthread_t tidMap[map_task_num];
  pthread_t tidReduce[reduce_task_num];
  for (int i = 0; i < map_task_num; i++) {
    pthread_create(&tidMap[i], NULL, mapWorker, NULL);
    pthread_detach(tidMap[i]);
  }
  pthread_mutex_lock(&map_mutex);
  pthread_cond_wait(&cond, &map_mutex);
  pthread_mutex_unlock(&map_mutex);
  for (int i = 0; i < reduce_task_num; i++) {
    pthread_create(&tidReduce[i], NULL, reduceWorker, NULL);
    pthread_detach(tidReduce[i]);
  }
  while (1) {
    if (work_client.call<bool>("IsAllMapAndReduceDone").val()) {
      break;
    }
    sleep(1);
  }

  //任务完成后清理中间文件，关闭打开的动态库，释放资源
  removeFiles();
  dlclose(handle);
  pthread_mutex_destroy(&map_mutex);
  pthread_cond_destroy(&cond);
}