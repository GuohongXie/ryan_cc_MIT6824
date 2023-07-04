#include "worker.h"

//static member initialsize
int Worker::map_id_ = 0;
std::mutex Worker::mutex_;
std::condition_variable Worker::cond_;

//TODO:Ihash有什么用
//对每个字符串求hash找到其对应要分配的reduce线程
int Worker::Ihash(std::string str) {
  int sum = 0;
  for (int i = 0; i < str.size(); i++) {
    sum += (str[i] - '0');  // TODO: not modern c++
  }
  return sum % reduce_task_num_;
}


/// @brief 删除所有写入中间值的临时文件
/// @brief 之所以不用任何参数是因为临时文件的命名格式固定
/// @brief 临时文件命名格式为"mr-i-j"
/// @brief 其中i为map_task_index, j为reduce_task_index
void Worker::RemoveTmpFiles() {
  std::string tmp_file_path;
  for (int i = 0; i < map_task_num_; i++) {
    for (int j = 0; j < reduce_task_num_; j++) {
      tmp_file_path = "mr-" + std::to_string(i) + "-" + std::to_string(j);
      int ret = ::access(tmp_file_path.c_str(), F_OK);
      if (ret == 0) ::remove(tmp_file_path.c_str()); // ret==0表示文件存在
    }
  }
}


/// @brief 取得  key:filename, value:content 的kv对作为map任务的输入
/// @param file_name 就是map任务要处理的文件的名字
/// @return key:filename, value:content 的kv对作为map任务的输入
KeyValue Worker::GetContent(const std::string& file_name) {
  int fd = ::open(file_name.c_str(), O_RDONLY);
  int length = ::lseek(fd, 0, SEEK_END);
  ::lseek(fd, 0, SEEK_SET);
  char buf[length];  //之所以用 char bug[]是为了匹配系统调用read
  ::bzero(buf, length);
  int len = ::read(fd, buf, length);
  if (len != length) {
    ::perror("read");
    ::exit(-1);
  }
  KeyValue kv;
  kv.key = std::string(file_name);
  kv.value = std::string(buf);
  ::close(fd);
  return kv;
}


/// @brief 将map任务产生的中间值写入临时文件
/// @param fd 
/// @param kv 
void Worker::WriteKV(int fd, const KeyValue& kv) {
  std::string tmp = kv.key + ",1 ";
  int len = ::write(fd, tmp.c_str(), tmp.size());
  if (len == -1) {
    ::perror("write");
    ::exit(-1);
  }
  ::close(fd);
}


/// @brief 创建每个map任务对应的不同reduce号的中间文件并调用 WriteKV 写入磁盘
/// @param kvs map_func产生的输出 
/// @param map_task_index 线程所对应的map_task_index，由MapWorker函数(线程启动函数)确定并传进参数于此 
void Worker::WriteInDisk(const std::vector<KeyValue>& kvs, int map_task_index) {
  for (const auto& kv : kvs) {
    int reduce_task_index = Ihash(kv.key); //reduce_task_index由kv.key的Ihash决定
    std::string path;
    path = "mr-" + std::to_string(map_task_index) + "-" + std::to_string(reduce_task_index);
    int ret = ::access(path.c_str(), F_OK);
    if (ret == -1) {
      int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
      WriteKV(fd, kv);
    } else if (ret == 0) {
      int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
      WriteKV(fd, kv);
    }
  }
}


/// @brief 以char类型的op为分割拆分字符串
/// @param text 
/// @param op 
/// @return 
std::vector<std::string> Worker::Split(std::string text, char op) {
  int n = text.size();
  std::vector<std::string> str;
  std::string tmp = "";
  for (int i = 0; i < n; i++) {
    if (text[i] != op) {
      tmp += text[i];
    } else {
      if (tmp.size() != 0) str.push_back(tmp);
      tmp = "";
    }
  }
  return str;
}

//以逗号为分割拆分字符串
std::string Worker::Split(std::string text) {
  std::string tmp = "";
  for (int i = 0; i < text.size(); i++) {
    if (text[i] != ',') {
      tmp += text[i];
    } else
      break;
  }
  return tmp;
}


/// @brief 获取对应reduce编号的所有中间文件
/// @param directory 
/// @param op 
/// @return 
std::vector<std::string> Worker::GetAllfile(std::string directory , int op) {
  DIR* dir = ::opendir(directory.c_str());
  std::vector<std::string> ret;
  if (dir == nullptr) {
    printf("[ERROR] %s is not a directory or not exist!", directory.c_str());
    return ret;
  }
  struct dirent* entry;
  while ((entry = ::readdir(dir)) != nullptr) {
    int len = strlen(entry->d_name);
    int oplen = std::to_string(op).size();
    if (len - oplen < 5) continue;
    std::string filename(entry->d_name);
    if (!(filename[0] == 'm' && filename[1] == 'r' &&
          filename[len - oplen - 1] == '-'))
      continue;
    std::string cmp_str = filename.substr(len - oplen, oplen);
    if (cmp_str == std::to_string(op)) {
      ret.push_back(entry->d_name);
    }
  }
  ::closedir(dir);
  return ret;
}

//对于一个ReduceTask，获取所有相关文件并将value的list以string写入vector
// vector中每个元素的形式为"abc 11111";
std::vector<KeyValue> Worker::MyShuffle(int reduce_task_num) {
  std::string path;
  std::vector<std::string> str;
  str.clear();  //TODO: 是否多余
  std::vector<std::string> filename = GetAllfile(".", reduce_task_num);
  std::unordered_map<std::string, std::string> hash;
  for (int i = 0; i < filename.size(); i++) {
    std::string text = filename[i];
    KeyValue kv = GetContent(text);
    std::string context = kv.value;
    std::vector<std::string> ret_str = Split(context, ' ');
    str.insert(str.end(), ret_str.begin(), ret_str.end());
  }
  for (const auto& a : str) {
    hash[Split(a)] += "1";
  }
  std::vector<KeyValue> ret_kvs;
  KeyValue tmpKv;
  for (const auto& a : hash) {
    tmpKv.key = a.first;
    tmpKv.value = a.second;
    ret_kvs.push_back(tmpKv);
  }
  std::sort(ret_kvs.begin(), ret_kvs.end(),
       [](KeyValue& kv1, KeyValue& kv2) { return kv1.key < kv2.key; });
  return ret_kvs;
}

void* Worker::MapWorker(void* arg) {
  // 1、初始化client连接用于后续RPC;获取自己唯一的MapTaskID
  buttonrpc map_worker_client;
  map_worker_client.as_client(kRpcCoordinatorServerIp_, kRpcCoordinatorServerPort_);
  std::unique_lock<std::mutex> lock(mutex_);
  int map_task_index = map_id_++;
  lock.unlock();
  bool ret = false;
  while (1) {
    // 2、通过RPC从Master获取任务
    // map_worker_client.set_timeout(10000);
    ret = map_worker_client.call<bool>("IsMapDone").val();
    if (ret) {
      cond_.notify_all(); // TODO: why?
      return nullptr;
    }
    std::string task_tmp = map_worker_client.call<std::string>("AssignTask").val();  //通过RPC返回值取得任务，在map中即为文件名
    if (task_tmp == "empty") continue;
    printf("%d get the task : %s\n", map_task_index, task_tmp.c_str());
    lock.lock();
    //std::unique_lock<std::mutex> lock(mutex);
    //------------------------自己写的测试超时重转发的部分---------------------
    //注：需要对应master所规定的map数量，因为是1，3，5被置为disabled，相当于第2，4，6个拿到任务的线程宕机
    //若只分配两个map的worker，即0工作，1宕机，我设的超时时间比较长且是一个任务拿完在拿一个任务，所有1的任务超时后都会给到0，
    //人为设置的crash线程，会导致超时，用于超时功能的测试
    if (disabled_map_id_ == 1 || disabled_map_id_ == 3 || disabled_map_id_ == 5) {
      disabled_map_id_++;
      lock.unlock();
      printf("%d recv task : %s  is stop\n", map_task_index, task_tmp.c_str());
      while (1) {
        sleep(2);
      } //TODO: why?
    } else {
      disabled_map_id_++;
    }
    lock.unlock();
    //------------------------自己写的测试超时重转发的部分---------------------

    // 3、拆分任务，任务返回为文件path及map任务编号，将filename及content封装到kv的key及value中
    const std::string task = task_tmp;
    KeyValue kv = GetContent(task);

    // 4、执行map函数，然后将中间值写入本地
    std::vector<KeyValue> kvs = map_func(kv);
    WriteInDisk(kvs, map_task_index);

    // 5、发送RPC给master告知任务已完成
    printf("%d finish the task : %s\n", map_task_index, task_tmp.c_str());
    map_worker_client.call<void>("SetMapStat", task_tmp);
  }
}

//用于最后写入磁盘的函数，输出最终结果
void Worker::MyWrite(int fd, std::vector<std::string>& str) {
  int len = 0;
  char buf[2];
  sprintf(buf, "\n");
  for (auto s : str) {
    len = write(fd, s.c_str(), s.size());
    write(fd, buf, strlen(buf));
    if (len == -1) {
      perror("write");
      exit(-1);
    }
  }
}

void* Worker::ReduceWorker(void* arg) {
  // RemoveTmpFiles();
  buttonrpc reduce_worker_client;
  reduce_worker_client.as_client(kRpcCoordinatorServerIp_, kRpcCoordinatorServerPort_);
  bool ret = false;
  while (1) {
    //若工作完成直接退出reduce的worker线程
    ret = reduce_worker_client.call<bool>("IsAllMapAndReduceDone").val();
    if (ret) {
      return nullptr;
    }
    int reduce_task_index = reduce_worker_client.call<int>("AssignReduceTask").val();
    if (reduce_task_index == -1) continue;
    printf("%ld get the task%d\n", pthread_self(), reduce_task_index);
    //TODO: mistake
    //printf("%ld get the task%d\n", std::to_string(std::thread::get_id()).c_str(), reduce_task_index);
    std::unique_lock<std::mutex> lock(mutex_);
    //人为设置的crash线程，会导致超时，用于超时功能的测试
    if (disabled_reduce_id_ == 1 || disabled_reduce_id_ == 3 ||
        disabled_reduce_id_ == 5) {
      disabled_reduce_id_++;
      lock.unlock();
      printf("recv task%d reduce_task_index is stop in %ld\n", reduce_task_index,
             pthread_self());
      while (1) {
        sleep(2);
      }
    } else {
      disabled_reduce_id_++;
    }
    lock.unlock();

    //取得reduce任务，读取对应文件，shuffle后调用reduceFunc进行reduce处理
    std::vector<KeyValue> kvs = MyShuffle(reduce_task_index);
    std::vector<std::string> ret = reduce_func(kvs, reduce_task_index);
    std::vector<std::string> str;
    for (int i = 0; i < kvs.size(); i++) {
      str.push_back(kvs[i].key + " " + ret[i]);
    }
    std::string filename = "mr-out-" + std::to_string(reduce_task_index);
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
    MyWrite(fd, str);
    close(fd);
    printf("%ld finish the task%d\n", pthread_self(), reduce_task_index);
    reduce_worker_client.call<bool>(
        "SetReduceStat",
        reduce_task_index);  //最终文件写入磁盘并发起RPCcall修改reduce状态
  }
}

//删除最终输出文件，用于程序第二次执行时清除上次保存的结果
void Worker::RemoveOutputFiles() {
  std::string path;
  for (int i = 0; i < MAX_REDUCE_NUM; i++) {
    path = "mr-out-" + std::to_string(i);
    int ret = access(path.c_str(), F_OK);
    if (ret == 0) remove(path.c_str());
  }
}

