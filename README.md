# MIT6.5840(原MIT6.824)的cpp实现
- MIT 6.824 distributed system C++ Version


## 项目介绍
MIT6.824的cpp实现, 大多数解释我都写到代码的注释里了，可直接看代码。
lab4还没有完成。


## 项目特点

- 使用 `C++` 的 `thread`、`mutex`、`condition_variable`、`semphore` 等底层同步原语进行多线程编程;
- 基于 `ZeroMQ` 实现了一个轻量级 `RPC`，提供客户端调用超时选项，客户端与服务端自动重连机制等;
- 实现了一个 `MapReduce` 并行处理框架，包含任务timeout重发机制、worker-stateless等机制;
- 实现了Raft共识算法，包括leader select、日志复制、日志压缩等机制;
- 基于以上 Raft 算法，实现了一个高可用的 KV 存储服务;
- 遵循 `RAII` 管理资源，如使用智能指针管理内存，减小`resource leak`风险。
- 使用 `gdb` 进行调试，使用 `clang` 系列工具进行代码静态检查和风格统一。

## 开发环境

- 操作系统：`Ubuntu 20.04 LTS`
- 编译器：`clang++ 10.0.0-4ubuntu1`
- 编辑器：`vscode`
- 版本控制：`git`
- 项目构建：`cmake 3.16.3`

## 构建项目
项目依赖
- `cmake`
```shell
sudo apt update
sudo apt install cmake
```
- `ZeroMQ` (for rpc)
```shell
sudo apt update
sudo apt install libzmq3-dev libczmq-dev
```

下载项目

```shell
git clone git@github.com:GuohongXie/ryan_cc_MIT6824.git
```

执行脚本构建项目

```shell
cd ./ryan_cc_MIT6824 && bash build.sh
```

## TODO

1. lab 4 完成得不是很好
2. 代码总体还比较粗糙，有很多值得优化的地方
3. mapreduce 未确保每个task的原子性，且可以补充一个心跳机制

## Acknowledgments

- [MIT6.824 home page](http://nil.csail.mit.edu/6.824/2020/)
- [课程中文翻译](https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/)
- [2020 MIT 6.824 分布式系统课程视频 哔哩哔哩_bilibili](https://www.bilibili.com/video/BV1R7411t71W/?p=1&vd_source=ffaa505c7eb259a9e1ec7629a770a957)
- [深入浅出讲解 MapReduce_哔哩哔哩_bilibili](https://www.bilibili.com/video/BV1Vb411m7go?spm_id_from=333.851.header_right.fav_list.click)

