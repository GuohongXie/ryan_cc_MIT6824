#include "map_reduce/mr/worker.h"

// static member variable initialsize


// 在mrworker的main线程中调用，用阻塞的方式等待所有map任务完成
// 因为当所有map task都完成之后，才能开始reduce任务
// 注意map task 和 map的工作线程是两个不同的概念
// map的工作线程负责处理一个map task，当线程超时时, 
//coordinator会将该map task重新分配给其他worker
void Worker::WaitForMapDone() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!is_map_done_) { // 你的实际检查条件
      cond_.wait(lock);
    }
  }

//对每个word(std::string)求hash找到其对应要分配的reduce线程
int Worker::Ihash(const std::string& str) const {
  int sum = 0;
  for (auto c : str) {
    sum += (c - '0');
  }
  return sum % reduce_task_num_;
}

/// @brief 删除所有写入中间值的临时文件
/// 之所以不用任何参数是因为临时文件的命名格式固定
/// 临时文件命名格式为"mr-i-j"
/// 其中i为map_task_index, j为reduce_task_index
void Worker::RemoveTmpFiles() const {
  for (int i = 0; i < map_task_num_; i++) {
    for (int j = 0; j < reduce_task_num_; j++) {
      std::string file_path_str =
          "mr-" + std::to_string(i) + "-" + std::to_string(j);
      std::filesystem::path file_path(
          file_path_str);  //<filesystem> is supported since c++17
      if (std::filesystem::exists(file_path)) {
        std::filesystem::remove(file_path);
      }
    }
  }
}

/// @brief 取得  key:filename, value:content 的kv对作为map任务的输入
/// @param file_name_str 就是map任务要处理的文件的名字
/// @return key:filename, value:content 的kv对作为map任务的输入
KeyValue Worker::GetContent(const std::string& file_name_str) {
  auto file = std::ifstream(std::filesystem::path(file_name_str));
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + file_name_str);
  }
  std::string content((std::istreambuf_iterator<char>(file)),
                      std::istreambuf_iterator<char>());
  // TODO: learning "most vexing parse"
  //最小解析规则(most vexing parse), 如果函数的第一个参数不加括号，
  //则此行代码会被解析为一个函数声明
  if (content.empty()) {
    throw std::runtime_error("Failed to read content from file: " +
                             file_name_str);
  }
  return {file_name_str, content};
}

/// @brief 将map任务产生的中间值写入临时文件, 由WriteInDisk函数调用
/// @param fd 临时文件的文件描述符, 由WriteInDisk函数传入
/// @param kv map任务产生的中间值, kv.key为word, kv.value为"1"
void Worker::WriteKV(int fd, const KeyValue& kv) {
  std::string tmp = kv.key + "," + kv.value + " ";
  ssize_t len = ::write(fd, tmp.c_str(), tmp.size());
  if (len == -1) {
    ::perror("write");
    ::exit(-1);
  }
  ::close(fd);
}

/// @brief 创建每个map任务对应的不同reduce号的中间文件并调用WriteKV写入磁盘, 由MapWorker函数(线程启动函数)调用
/// @param kvs map_func产生的输出, 即key:word, value:"1"
/// @param map_task_index 线程所对应的map_task_index，由MapWorker函数(线程启动函数)确定并传进参数于此
void Worker::WriteInDisk(const std::vector<KeyValue>& kvs, int map_task_index) {
  for (const auto& kv : kvs) {
    int reduce_task_index =
        Ihash(kv.key);  // reduce_task_index由kv.key的Ihash决定
    std::string path;
    path = "mr-" + std::to_string(map_task_index) + "-" +
           std::to_string(reduce_task_index);
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

/// @brief 以char类型的seporator为分割拆分字符串, 由MyShuffle函数调用
/// @param text 从中间文件读取的字符串, 形式为"word,1 word,1 word,1 ..."
/// @param seporator 分割符, 由ReduceWorker函数(线程启动函数)传入
/// @return 拆分后的字符串数组, 形式为["word,1", "word,1", "word,1", ...]
std::vector<std::string> Worker::Split(const std::string& text_str, char seporator) {
  std::string_view text = text_str;
  std::vector<std::string> result;
  size_t start = 0;
  size_t end = text.find(seporator, start);
  while (end != std::string_view::npos) {
    result.emplace_back(text.substr(start, end - start));
    start = end + 1;
    end = text.find(seporator, start);
  }
  result.emplace_back(text.substr(start, end));
  return result;
}


/// @brief 将传入的"word, 1"中的"word"返回，作为MyShuffle函数中umap的key
/// @param text "word, 1"
/// @return "word"
std::string Worker::Split(const std::string& text) {
  std::string tmp;
  for (char c : text) {
    if (c != ',') {
      tmp += c;
    } else
      break;
  }
  return tmp;
}

/// @brief 获取对应reduce编号的所有中间文件, 由ReduceWorker函数(线程启动函数)调用
/// @param directory_str  中间文件所在的目录
/// @param op reduce编号
/// @return 对应reduce编号的所有中间文件
std::vector<std::string> Worker::GetAllfile(const std::string& directory_str,
                                            int reduce_task_index) {
  auto directory = std::filesystem::path(directory_str);
  std::vector<std::string> ret;
  if (!std::filesystem::is_directory(directory)) {
    printf("[ERROR] %s is not a directory or not exist!",
           directory.string().c_str());
    return ret;
  }
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    std::string filename = entry.path().filename().string();
    std::string prefix = "mr-";
    std::string suffix = "-" + std::to_string(reduce_task_index);
    if (filename.compare(0, prefix.size(), prefix) == 0 &&
        filename.compare(filename.size() - suffix.size(), suffix.size(),
                         suffix) == 0) {
      ret.push_back(filename);
    }
  }
  return ret;
}

//对于一个ReduceTask，获取所有相关文件并将value的list以string写入vector
// vector中每个元素的形式为"abc 11111";

/// @brief 对于一个ReduceTask，获取所有相关文件并将value的list以string写入vector
/// 此函数由ReduceWorker函数(线程启动函数)调用
/// @param reduce_task_index reduce编号
/// @return {["worda":"11111"], ["wordb":"111"], ["wordc":"1111"], ...}
/// 其中，key为word, value为"11111"
std::vector<KeyValue> Worker::MyShuffle(int reduce_task_index) {
  std::vector<std::string> filenames = GetAllfile(".", reduce_task_index);
  std::unordered_map<std::string, std::string> umap;
  // umap: "word" -> "11111..."
  std::vector<std::string> all_contents;
  // all_contents: ["worda, 1", "worda, 1", "wordb, 1", "wordb, 1", ...]
  for (const auto& filename : filenames) {
    KeyValue kv = GetContent(filename); // key:filename, value:file content
    // content: "word,1 word,1 word,1 ..."
    std::string content = kv.value;
    auto ret_str = Split(content, ' ');
    all_contents.insert(all_contents.end(), ret_str.begin(), ret_str.end());
  }
  for (const auto& s : all_contents) {
    umap[Split(s)] += "1";
  }
  std::vector<KeyValue> ret_kvs;
  ret_kvs.reserve(umap.size());
  for (const auto& it : umap) {
    ret_kvs.emplace_back(it.first, it.second);
  }
  std::sort(ret_kvs.begin(), ret_kvs.end(),
            [](const KeyValue& kv1, const KeyValue& kv2) {
              return kv1.key < kv2.key;
            });
  return ret_kvs;
}


void Worker::MapWorker() {
  // 1、初始化client连接用于后续RPC;获取自己唯一的MapTaskID
  buttonrpc map_worker_client;
  map_worker_client.as_client(kRpcCoordinatorServerIp_,
                              kRpcCoordinatorServerPort_);
  std::unique_lock<std::mutex> lock(mutex_);
  int map_task_index = map_id_++;
  lock.unlock();

  while (true) {
    // 2、通过RPC从Master获取任务
    bool ret = map_worker_client.call<bool>("IsMapDone").val();
    if (ret) {
      // 如果所有map任务都已经完成，通知coordinator, 并退出MapWorker
      // cond_.notify_all() 会唤醒所有等待在cond_上的线程，此处是唤醒mrworker主线程
      is_map_done_ = true;
      NotifyMapDone();
      //cond_.notify_all();
      return;
    }
    //通过RPC返回值取得任务，在Map中即为要处理的文件名 
    // map_task_name 是文件名，如 "pg-being_ernest.txt"
    // map_task_name = "empty" 表示没有任务，继续等待
    std::string map_task_name = map_worker_client.call<std::string>("AssignMapTask").val();
    if (map_task_name == "empty") continue;
    std::cout << "MapWorker " << map_task_index << " get task: " << map_task_name << std::endl;

    lock.lock();
    //------------------------自己写的测试超时重转发的部分---------------------
    //注：需要对应master所规定的map数量，因为是1，3，5被置为disabled，相当于第2，4，6个拿到任务的线程宕机
    //若只分配两个map的worker，即0工作，1宕机，我设的超时时间比较长且是一个任务拿完在拿一个任务，所有1的任务超时后都会给到0，
    //人为设置的crash线程，会导致超时，用于超时功能的测试
    if (disabled_map_id_ == 1 || disabled_map_id_ == 3 ||
        disabled_map_id_ == 5) {
      ++disabled_map_id_;
      lock.unlock();
      std::cout << "MapWorker " << map_task_index << " having received task: " << map_task_name << " is timeout and stop"
                << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;  // while(true) { ::sleep(2); }
    }
    ++disabled_map_id_;
    lock.unlock();

    //3、拆分任务，任务返回为文件path及map任务编号，将filename及content封装到kv的key及value中
    auto kv = GetContent(map_task_name);
    // 4、执行map函数，然后将中间值写入本地
    auto kvs = map_func(kv);
    WriteInDisk(kvs, map_task_index);

    // 5、发送RPC给master告知任务已完成
    std::cout << "MapWorker " << map_task_index << " finish task: " << map_task_name << std::endl;  // 相当于打印日志log
    map_worker_client.call<void>("SetAMapTaskFinished", map_task_name);
  }
}

//用于最后写入磁盘的函数，输出最终结果, 由ReduceWorker调用
//
void Worker::MyWrite(int fd, std::vector<std::string>& strs) {
  for (const auto& s : strs) {
    auto len = ::write(fd, s.c_str(), s.size());
    ::write(fd, "\n", 1);
    if (len == -1) {
      std::perror("write");
      std::exit(-1);
    }
  }
}


void Worker::ReduceWorker() {
  // 初始化RPC用于后续的RPC调用
  buttonrpc reduce_worker_client;
  reduce_worker_client.as_client(kRpcCoordinatorServerIp_,
                                 kRpcCoordinatorServerPort_);

  while (true) {
    //若工作完成直接退出reduce的worker线程
    bool ret = reduce_worker_client.call<bool>("IsAllMapAndReduceDone").val();
    if (ret) return;

    int reduce_task_index = reduce_worker_client.call<int>("AssignReduceTask").val();
    // 如果没有任务，继续循环
    if (reduce_task_index == -1) continue;
    std::cout << "ReduceWorker " << reduce_task_index << " is working on thread: "
              << std::this_thread::get_id() << std::endl;

    std::unique_lock<std::mutex> lock(mutex_);
    //人为设置的crash线程，会导致超时，用于超时功能的测试
    if (disabled_reduce_id_ == 1 || disabled_reduce_id_ == 3 ||
        disabled_reduce_id_ == 5) {
      ++disabled_reduce_id_;
      lock.unlock();
      std::cout << "ReduceWorker " << reduce_task_index
                << " is timeout and stop on thread: "
                << std::this_thread::get_id() << std::endl;
      while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
      }
    }
    ++disabled_reduce_id_;
    lock.unlock();

    //取得reduce任务，读取对应文件，shuffle后调用reduceFunc进行reduce处理
    std::vector<KeyValue> kvs = MyShuffle(reduce_task_index);
    // MyShuffle将reduce_task_index对应的文件读取到kvs中
    // reduce_task_index对应哪些文件，在MapWorker中由Ihash得到
    // 故相同的word会被分配到相同的文件中
    // MyShuffle根据reduce_task_index找到对应的文件名,然后读取文件内容
    // 文件内容为 "worda,1 worda,1 wordb,1 wordc,1 ..."
    // MyShuffle 将所有文件内容合并，并排序到一个vector中，形式为
    // kvs : {["worda":"11111"], ["wordb":"111"], ["wordc":"1111"], ...}
    std::vector<std::string> occurance = reduce_func(kvs, reduce_task_index);
    // occurance : {"5", "3", "4", ...}
    std::vector<std::string> word_and_occurance;
    // word_and_occurance : {"worda 5", "wordb 3", "wordc 4", ...}
    for (size_t i = 0; i < kvs.size(); i++) {
      word_and_occurance.push_back(kvs[i].key + " " + occurance[i]);
    }

    //最终文件写入磁盘并发起RPCcall修改reduce状态
    std::string filename = "mr-out-" + std::to_string(reduce_task_index);
    int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
    MyWrite(fd, word_and_occurance);
    ::close(fd);

    std::cout << "ReduceWorker " << reduce_task_index << " is done on thread: "
              << std::this_thread::get_id() << std::endl;
    reduce_worker_client.call<bool>("SetAReduceTaskFinished", reduce_task_index);
  }
}

//删除最终输出文件，用于程序第二次执行时清除上次保存的结果
//同样由于文件名固定，所以不需要传入参数
void Worker::RemoveOutputFiles() {
  for (int i = 0; i < MAX_REDUCE_NUM; i++) {
    std::string path = "mr-out-" + std::to_string(i);
    if (std::filesystem::exists(path)) {
      std::filesystem::remove(path);
    }
  }
}
