#ifndef RYAN_DS_BUTTONRPC_SERIALIZER_HPP_
#define RYAN_DS_BUTTONRPC_SERIALIZER_HPP_

#include <cstring>
#include <algorithm>
#include <cstdint>
#include <sstream>
#include <vector>

class StreamBuffer : public std::vector<char> {
 public:
  StreamBuffer() { curr_pos_ = 0; }
  StreamBuffer(const char* in, size_t len) {
    curr_pos_ = 0;
    insert(begin(), in, in + len);
  }
  ~StreamBuffer()= default;

  void reset() { curr_pos_ = 0; }
  const char* data() { return &(*this)[0]; }
  const char* current() { return &(*this)[curr_pos_]; }
  void offset(int k) { curr_pos_ += k; }
  bool is_eof() { return (curr_pos_ >= size()); }
  void input(char* in, size_t len) { insert(end(), in, in + len); }
  int findc(char c) {
    auto itr = find(begin() + curr_pos_, end(), c);
    if (itr != end()) {
      return static_cast<int>(itr - (begin() + curr_pos_));
    }
    return -1;
  }

 private:
  // 
  unsigned int curr_pos_;
};

class Serializer {
 public:
  Serializer() { byte_order_ = LittleEndian; };
  ~Serializer()= default;;

  explicit Serializer(const StreamBuffer& dev, int byteorder = LittleEndian) {
    byte_order_ = byteorder;
    io_device_ = dev;
  }

 public:
  enum ByteOrder { BigEndian, LittleEndian };

 public:
  void reset() { io_device_.reset(); }
  int size() { return io_device_.size(); }
  void skip_raw_date(int k) { io_device_.offset(k); }
  const char* data() { return io_device_.data(); }
  void byte_orser(char* in, int len) const {
    if (byte_order_ == BigEndian) {
      std::reverse(in, in + len);
    }
  }
  void write_raw_data(char* in, int len) {
    io_device_.input(in, len);
    io_device_.offset(len);
  }
  const char* current() { return io_device_.current(); }
  void clear() {
    io_device_.clear();
    reset();
  }

  template <typename T>
  void output_type(T& t);

  template <typename T>
  void input_type(T t);

  // 
  void get_length_mem(char* p, int len) {
    std::memcpy(p, io_device_.current(), len);
    io_device_.offset(len);
  }

 public:
  template <typename T>
  Serializer& operator>>(T& i) {
    output_type(i);
    return *this;
  }

  template <typename T>
  Serializer& operator<<(T i) {
    input_type(i);
    return *this;
  }

 private:
  int byte_order_;
  StreamBuffer io_device_;
};

template <typename T>
inline void Serializer::output_type(T& t) {
  int len = sizeof(T);
  char* d = new char[len];
  if (!io_device_.is_eof()) {
    std::memcpy(d, io_device_.current(), len);
    io_device_.offset(len);
    byte_orser(d, len);
    t = *reinterpret_cast<T*>(&d[0]);
  }
  delete[] d;
}

template <>
inline void Serializer::output_type(std::string& in) {
  int marklen = sizeof(uint16_t);
  char* d = new char[marklen];
  std::memcpy(d, io_device_.current(), marklen);
  byte_orser(d, marklen);
  int len = *reinterpret_cast<uint16_t*>(&d[0]);
  io_device_.offset(marklen);
  delete[] d;
  if (len == 0) return;
  in.insert(in.begin(), io_device_.current(), io_device_.current() + len);
  io_device_.offset(len);
}

template <typename T>
inline void Serializer::input_type(T t) {
  int len = sizeof(T);
  char* d = new char[len];
  const char* p = reinterpret_cast<const char*>(&t);
  std::memcpy(d, p, len);
  byte_orser(d, len);
  io_device_.input(d, len);
  delete[] d;
}

template <>
inline void Serializer::input_type(std::string in) {
  // 
  uint16_t len = in.size();
  char* p = reinterpret_cast<char*>(&len);
  byte_orser(p, sizeof(uint16_t));
  io_device_.input(p, sizeof(uint16_t));

  // 
  if (len == 0) return;
  char* d = new char[len];
  std::memcpy(d, in.c_str(), len);
  io_device_.input(d, len);
  delete[] d;
}

template <>
inline void Serializer::input_type(const char* in) {
  input_type<std::string>(std::string(in));
}

#endif //RYAN_DS_BUTTONRPC_SERIALIZER_HPP_