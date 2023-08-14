#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

struct KeyValue {
  std::string key;
  std::string value;
  KeyValue(std::string  k, std::string  v) : key(std::move(k)), value(std::move(v)) {}
};

/// @brief split content of a document into words, and store them in a vector
/// will be used in map_func
/// @param text content of a document
/// @return the splited words
//有点像leetcode的题目, time: O(n), space: O(n)
std::vector<std::string> SplitTextIntoWords(const std::string& text) {
  std::vector<std::string> words;
  std::string word;
  for (char c : text) { // don't need to use reference
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

/// @brief MapFunc，需要打包成动态库，并在worker中通过dlopen以及dlsym运行时加载
/// 一个MapFunc一次处理一个document
/// @param kv kv.key: document name; kv.value: document content
/// @return 
extern "C" std::vector<KeyValue> MapFunc(const KeyValue& kv) {
  std::vector<KeyValue> result;
  std::vector<std::string> words = SplitTextIntoWords(kv.value);
  result.reserve(words.size());
  for (const auto& word : words) {
    result.emplace_back(word, "1");
  }
  return result;
}

/// @brief ReduceFunc，也是动态加载，输出对特定keyValue的reduce结果
/// @param kvs: {["worda":"11111"], ["wordb":"111"], ["wordc":"1111"], ...}
/// @return {"5", "3", "4", ...}，每个key对应的value是该key出现的次数
extern "C" std::vector<std::string> ReduceFunc(const std::vector<KeyValue>& kvs) {
  // kvs[0].key: a word
  // kvs[0].value: a list of counts
  std::vector<std::string> result;
  result.reserve(kvs.size());
for (const auto& kv : kvs) {
    result.push_back(std::to_string(kv.value.size()));  // count how many "1"
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