#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

struct KeyValue {
  std::string key;
  std::string value;
};

/// @brief 自己写的字符串按照单词分割函数，因为mapReduce中经常用到
/// @param text 传入的文本内容，数据量极大
/// @return std::vector<std::string> 返回各个分割后的单词列表

std::vector<std::string> SplitTextIntoWords(const std::string& text) {
  std::vector<std::string> words;
  std::string word;
  for (char c : text) {
    if (std::isalpha(c)) {
      word += c;
    } else {
      if (!word.empty()) {
        words.push_back(word);
        word.clear();
      }
    }
  }
  if (!word.empty()) {
    words.push_back(word);
  }
  return words;
}


/// @brief map_func，需要打包成动态库，并在worker中通过dlopen以及dlsym运行时加载
/// @param kv 将文本按单词划分并以出现次数代表value长度存入keyValue
/// @return 类似{"my 11111", "you 111"} 即文章中my出现了5次，you出现了3次


extern "C" std::vector<KeyValue> MapFunc(const KeyValue& kv) {
  std::vector<KeyValue> result;
  std::vector<std::string> words = SplitTextIntoWords(kv.value);
  for (const auto& word : words) {
    KeyValue tmp;
    tmp.key = word;
    tmp.value = "1";
    result.emplace_back(tmp);
  }
  return result;
}

/// @brief reduce_func，也是动态加载，输出对特定keyValue的reduce结果
/// @param kvs 
/// @return std::vector<std::string>

extern "C" std::vector<std::string> ReduceFunc(const std::vector<KeyValue>& kvs) {
  std::vector<std::string> result;
  for (const auto& kv : kvs) {
    result.push_back(std::to_string(kv.value.size()));
  }
  return result;
}


#if 0
int main() {
  // Example usage
  KeyValue kv;
  kv.value = "Hello world!";
  std::vector<KeyValue> mapped = MapFunc(kv);
  std::vector<std::string> reduced = ReduceFunc(mapped);
  for (const auto& str : reduced) {
    std::cout << str << std::endl;
  }
  return 0;
}
#endif