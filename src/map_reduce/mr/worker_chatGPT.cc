#include "worker.h"
#include <filesystem>
#include <fstream>
#include <thread>

//static member initialsize
int Worker::map_id_ = 0;
std::mutex Worker::mutex_;
std::condition_variable Worker::cond_;

int Worker::Ihash(std::string_view str) {
  int sum = 0;
  for (auto c : str) {
    sum += (c - '0');
  }
  return sum % reduce_task_num_;
}

void Worker::RemoveTmpFiles() {
  for (int i = 0; i < map_task_num_; i++) {
    for (int j = 0; j < reduce_task_num_; j++) {
      std::ostringstream tmp_file_path;
      tmp_file_path << "mr-" << i << "-" << j;
      std::filesystem::path file_path(tmp_file_path.str());
      if (std::filesystem::exists(file_path)) {
        std::filesystem::remove(file_path);
      }
    }
  }
}

KeyValue Worker::GetContent(const std::filesystem::path& file_path) {
  std::ifstream file(file_path);
  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  return {file_path.string(), content};
}

void Worker::WriteKV(std::ofstream& out_file, const KeyValue& kv) {
  std::string tmp = kv.key + ",1 ";
  out_file << tmp;
  if (!out_file) {
    perror("write");
    exit(-1);
  }
}

void Worker::WriteInDisk(const std::vector<KeyValue>& kvs, int map_task_index) {
  for (const auto& kv : kvs) {
    int reduce_task_index = Ihash(kv.key);
    std::ostringstream path_stream;
    path_stream << "mr-" << map_task_index << "-" << reduce_task_index;
    std::filesystem::path file_path(path_stream.str());

    std::ofstream out_file;
    if (std::filesystem::exists(file_path)) {
      out_file.open(file_path, std::ios_base::app);
    } else {
      out_file.open(file_path);
    }
    WriteKV(out_file, kv);
  }
}

std::vector<std::string> Worker::Split(std::string_view text, char op) {
  std::vector<std::string> result;
  size_t start = 0;
  size_t end = text.find(op);
  while (end != std::string_view::npos) {
    result.push_back(std::string(text.substr(start, end - start)));
    start = end + 1;
    end = text.find(op, start);
  }
  result.push_back(std::string(text.substr(start, end)));
  return result;
}

std::vector<std::string> Worker::GetAllfile(const std::filesystem::path& directory, int op) {
  std::vector<std::string> ret;
  if (!std::filesystem::is_directory(directory)) {
    printf("[ERROR] %s is not a directory or not exist!", directory.string().c_str());
    return ret;
  }
  
  for (const auto & entry : std::filesystem::directory_iterator(directory)) {
    std::string filename = entry.path().filename().string();
    if (filename.starts_with("mr-") && filename.ends_with("-" + std::to_string(op))) {
      ret.push_back(filename);
    }
  }
  return ret;
}

// 更多的函数实现...

