#ifndef RYAN_DS_BUTTONRPC_BUTTONRPC_HPP_
#define RYAN_DS_BUTTONRPC_BUTTONRPC_HPP_
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include "buttonrpc/serializer.hpp"

template <typename T>
struct type_xx {
  using type = T;

};

template <>
struct type_xx<void> {
  using type = int8_t;

};

class buttonrpc {
 public:
  enum rpc_role { RPC_CLIENT, RPC_SERVER };
  enum rpc_err_code {
    RPC_ERR_SUCCESS = 0,
    RPC_ERR_FUNCTIION_NOT_BIND,
    RPC_ERR_RECV_TIMEOUT
  };
  // return value
  template <typename T>
  class value_t {
   public:
    //typedef typename type_xx<T>::type type;
    //typedef std::string msg_type;
    //typedef uint16_t code_type;
    using type = typename type_xx<T>::type;
    using msg_type = std::string;
    using code_type = uint16_t;


    value_t() {
      code_ = 0;
      msg_.clear();
    }
    bool valid() { return (code_ == 0 ? true : false); }
    int error_code() { return code_; }
    std::string error_msg() { return msg_; }
    type val() { return val_; }

    void set_val(const type& val) { val_ = val; }
    void set_code(code_type code) { code_ = code; }
    void set_msg(msg_type msg) { msg_ = msg; }

    friend Serializer& operator>>(Serializer& in, value_t<T>& d) {
      in >> d.code_ >> d.msg_;
      if (d.code_ == 0) {
        in >> d.val_;
      }
      return in;
    }
    friend Serializer& operator<<(Serializer& out, value_t<T> d) {
      out << d.code_ << d.msg_ << d.val_;
      return out;
    }

   private:
    code_type code_;
    msg_type msg_;
    type val_;
  };

  buttonrpc();
  ~buttonrpc();

  // network
  void as_client(const std::string& ip, int port);
  void as_server(int port);
  void send(zmq::message_t& data);
  void recv(zmq::message_t& data);
  void set_timeout(uint32_t ms);
  void run();

 public:
  // server
  template <typename F>
  void bind(const std::string& name, F func);

  template <typename F, typename S>
  void bind(const std::string& name, F func, S* s);

  template <typename F, typename S>
  void bind(const std::string& name, F func, const S* constObj);

  // client
  // 原来是七八个call的重载函数，现在使用可变模板参数的call函数 
  template <typename R, typename... Args>
  value_t<R> call(const std::string& name, Args... args);


 private:
  // *******以下四个是序列化和反序列化的辅助函数********
  // ******Serialize******
  // base case
  void serialize_helper(Serializer& ds) {}
  // recursive case
  template <typename First, typename... Rest>
  void serialize_helper(Serializer& ds, First first, Rest... rest);
  // *****Deserialize******
  // base case
  template<typename SerializerType>
  void extract_args(SerializerType&) {}
  // recursive case
  template<typename SerializerType, typename First, typename... Rest>
  void extract_args(SerializerType& ds, First& first, Rest&... rest);


  Serializer* call_(const std::string& name, const char* data, int len);

  template <typename R>
  value_t<R> net_call(Serializer& ds);

  template <typename F>
  void callproxy(F fun, Serializer* pr, const char* data, int len);

  template <typename F, typename S>
  void callproxy(F fun, S* s, Serializer* pr, const char* data, int len);

  //for const-memfunc
  template <typename F, typename S>
  void callproxy(F fun, const S* s, Serializer* pr, const char* data, int len);


  // PROXY FUNCTION POINT
  template <typename R, typename... Args>
  void callproxy_func_(R (*func)(Args...), Serializer* pr, const char* data, int len) {
    callproxy_functional_(std::function<R(Args...)>(func), pr, data, len);
  }


  // PROXY CLASS MEMBER
  template <typename R, typename C, typename S, typename... Args>
  void callproxy_memfunc_(R (C::*func)(Args...), S* s, Serializer* pr, const char* data, int len) {
    callproxy_functional_(std::function<R(Args...)>(
        [=](Args... args) { return (s->*func)(args...); }
    ), pr, data, len);
  }
  // for const-memfunc
  template <typename R, typename C, typename S, typename... Args>
  void callproxy_memfunc_(R (C::*func)(Args...) const, const S* s, Serializer* pr, const char* data, int len) {
    callproxy_functional_(std::function<R(Args...)>(
      [=](Args... args) { return (s->*func)(args...); }
    ), pr, data, len);
  }


  // PORXY FUNCTIONAL
  //callproxy_functional_只需要处理最终的函数对象
  //它不需要直到函数对象是否const，因为std::function会自动处理 
  //所以它不需要重载
  template <typename R, typename... Params>
  void callproxy_functional_(std::function<R(Params...)> func, Serializer* pr,
                           const char* data, int len);

 private:
  std::map<std::string, std::function<void(Serializer*, const char*, int)>>
      name_func_map_;

  zmq::context_t context_;
  zmq::socket_t* socket_;

  rpc_err_code error_code_;

  int role_;
};

buttonrpc::buttonrpc() : context_(1) { error_code_ = RPC_ERR_SUCCESS; }

buttonrpc::~buttonrpc() {
  socket_->close();
  delete socket_;
  context_.close();
}

// network
// TODO:ip和port的异常处理
void buttonrpc::as_client(const std::string& ip, int port) {
  role_ = RPC_CLIENT;
  socket_ = new zmq::socket_t(context_, ZMQ_REQ);
  std::ostringstream os;
  os << "tcp://" << ip << ":" << port;
  socket_->connect(os.str());
}

//TODO: port的异常处理
void buttonrpc::as_server(int port) {
  role_ = RPC_SERVER;
  socket_ = new zmq::socket_t(context_, ZMQ_REP);
  std::ostringstream os;
  os << "tcp://*:" << port;
  socket_->bind(os.str());
}

void buttonrpc::send(zmq::message_t& data) { socket_->send(data); }

void buttonrpc::recv(zmq::message_t& data) { socket_->recv(&data); }

inline void buttonrpc::set_timeout(uint32_t ms) {
  // only client can set
  if (role_ == RPC_CLIENT) {
    socket_->setsockopt(ZMQ_RCVTIMEO, ms);
  }
}

void buttonrpc::run() {
  // only server can call
  if (role_ != RPC_SERVER) {
    return;
  }
  while (true) {
    zmq::message_t data;
    this->recv(data);
    StreamBuffer iodev((char*)data.data(), data.size());
    Serializer ds(iodev);

    std::string funname;
    ds >> funname;
    Serializer* r = this->call_(funname, ds.current(), ds.size() - funname.size());

    zmq::message_t retmsg(r->size());
    std::memcpy(retmsg.data(), r->data(), r->size());
    this->send(retmsg);
    delete r;
  }
}

// private

Serializer* buttonrpc::call_(const std::string& name, const char* data, int len) {
  Serializer* ds = new Serializer();
  if (name_func_map_.find(name) == name_func_map_.end()) {
    (*ds) << value_t<int>::code_type(RPC_ERR_FUNCTIION_NOT_BIND);
    (*ds) << value_t<int>::msg_type("function not bind: " + name);
    return ds;
  }
  auto fun = name_func_map_[name];
  fun(ds, data, len);
  ds->reset();
  return ds;
}


template <typename F>
void buttonrpc::bind(const std::string& name, F func) {
  name_func_map_[name] = [this, func](auto&& arg1, auto&& arg2, auto&& arg3) {
    return this->callproxy<F>(func, 
                              std::forward<decltype(arg1)>(arg1), 
                              std::forward<decltype(arg2)>(arg2), 
                              std::forward<decltype(arg3)>(arg3)
                             );
    };
}

template <typename F, typename S>
void buttonrpc::bind(const std::string& name, F func, S* s) {
  name_func_map_[name] = [this, func, s](auto&& arg1, auto&& arg2, auto&& arg3) {
    return this->callproxy<F, S>(func, 
                                 s, 
                                 std::forward<decltype(arg1)>(arg1), 
                                 std::forward<decltype(arg2)>(arg2), 
                                 std::forward<decltype(arg3)>(arg3)
                                );
    };
}

// for const-memfunc
template <typename F, typename S>
void buttonrpc::bind(const std::string& name, F func, const S* constObj) {
  name_func_map_[name] = [this, func, constObj](auto&& arg1, auto&& arg2, auto&& arg3) {
    return this->callproxy<F, const S>(func, 
                                       constObj, 
                                       std::forward<decltype(arg1)>(arg1), 
                                       std::forward<decltype(arg2)>(arg2), 
                                       std::forward<decltype(arg3)>(arg3)
                                      );
    };
}



template <typename F>
void buttonrpc::callproxy(F fun, Serializer* pr, const char* data, int len) {
  callproxy_func_(fun, pr, data, len);
}

template <typename F, typename S>
inline void buttonrpc::callproxy(F fun, S* s, Serializer* pr, const char* data,
                                 int len) {
  callproxy_memfunc_(fun, s, pr, data, len);
}

template <typename F, typename S>
inline void buttonrpc::callproxy(F fun, const S* s, Serializer* pr, const char* data, int len) {
    callproxy_memfunc_(fun, s, pr, data, len);
}



// help call return value type is void function
template <typename R, typename F>
typename std::enable_if<std::is_same<R, void>::value,
                        typename type_xx<R>::type>::type
call_helper(F f) {
  f();
  return 0;
}

template <typename R, typename F>
typename std::enable_if<!std::is_same<R, void>::value,
                        typename type_xx<R>::type>::type
call_helper(F f) {
  return f();
}


// callproxy_functional_
template<typename SerializerType, typename First, typename... Rest>
void buttonrpc::extract_args(SerializerType& ds, First& first, Rest&... rest) {
    ds >> first;
    extract_args(ds, rest...);
}
// 原来是七八个callproxy_functional_的重载函数，现在使用可变模板参数的callproxy_functional_函数
template <typename R, typename... Params>
void buttonrpc::callproxy_functional_(std::function<R(Params...)> func, Serializer* pr,
                           const char* data, int len) {
    Serializer ds(StreamBuffer(data, len));
    std::tuple<Params...> args;  // 存储所有参数
    std::apply([this, &ds](auto&... a) { this->extract_args(ds, a...); }, args);

    // 使用std::apply调用func
    typename type_xx<R>::type r = call_helper<R>([&]() {
        return std::apply(func, args);
    });

    value_t<R> val;
    val.set_code(RPC_ERR_SUCCESS);
    val.set_val(r);
    (*pr) << val;
}


template <typename R>
inline buttonrpc::value_t<R> buttonrpc::net_call(Serializer& ds) {
  zmq::message_t request(ds.size() + 1);
  memcpy(request.data(), ds.data(), ds.size());
  if (error_code_ != RPC_ERR_RECV_TIMEOUT) {
    send(request);
  }
  zmq::message_t reply;
  recv(reply);
  value_t<R> val;
  if (reply.size() == 0) {
    // timeout
    error_code_ = RPC_ERR_RECV_TIMEOUT;
    val.set_code(RPC_ERR_RECV_TIMEOUT);
    val.set_msg("recv timeout");
    return val;
  }
  error_code_ = RPC_ERR_SUCCESS;
  ds.clear();
  ds.write_raw_data((char*)reply.data(), reply.size());
  ds.reset();

  ds >> val;
  return val;
}

// client call
template <typename First, typename... Rest>
void buttonrpc::serialize_helper(Serializer& ds, First first, Rest... rest) {
    ds << first;
    serialize_helper(ds, rest...);
}

// 使用可变模板参数的call函数
// 原来是七八个call的重载函数，现在使用可变模板参数的call函数 
template <typename R, typename... Args>
buttonrpc::value_t<R> buttonrpc::call(const std::string& name, Args... args) {
    Serializer ds;
    ds << name;
    serialize_helper(ds, args...);
    return net_call<R>(ds);
}

#endif //RYAN_DS_BUTTONRPC_BUTTONRPC_HPP_