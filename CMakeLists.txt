# 指定使用 CMake 的最低版本号
cmake_minimum_required(VERSION 3.0)

# 设置项目名称
project(ryan_cc_mit_6_824 C CXX)

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

# 设置编译选项
set(compile_flags "-g -Wall -std=c++17 -O0")
set(CMAKE_CXX_FLAGS"${CMAKE_CXX_FLAGS} ${compile_flags}")

#find_package(Boost REQUIRED)
#find_library(BOOST_TEST_LIBRARY NAMES boost_unit_test_framework)
#include_directories(${Boost_INCLUDE_DIRS})


# 添加搜索路径
include_directories(
    ${PROJECT_SOURCE_DIR}/src/
    #${PROJECT_SOURCE_DIR}/src/buttonrpc/depends
    )

# 定义cc文件的路径
## map_reduce
file(GLOB CC_MR_WORD_COUNT ${PROJECT_SOURCE_DIR}/src/map_reduce/mrapps/word_count.cc)
file(GLOB CC_COORDINATOR ${PROJECT_SOURCE_DIR}/src/map_reduce/mr/coordinator.cc)
file(GLOB CC_WORKER ${PROJECT_SOURCE_DIR}/src/map_reduce/mr/worker.cc)
file(GLOB MAIN_COORDINATOR ${PROJECT_SOURCE_DIR}/src/map_reduce/main/mrcoordinator.cc)
file(GLOB MAIN_WORKER ${PROJECT_SOURCE_DIR}/src/map_reduce/main/mrworker.cc)
## raft
file(GLOB MAIN_RAFT ${PROJECT_SOURCE_DIR}/src/raft/raft.cc)
## kv_raft
file(GLOB MAIN_KV_RAFT_SERVER ${PROJECT_SOURCE_DIR}/src/kv_raft/server.cc)
file(GLOB MAIN_KV_RAFT_CLIENT ${PROJECT_SOURCE_DIR}/src/kv_raft/client.cc)


# 生成可执行文件
## map_reduce
add_library(mr_word_count SHARED ${CC_MR_WORD_COUNT})
add_executable(mrcoordinator ${MAIN_COORDINATOR} ${CC_COORDINATOR})
target_link_libraries(mrcoordinator pthread -lzmq)
add_executable(mrworker ${MAIN_WORKER})
target_link_libraries(mrworker pthread -ldl -lzmq)
## raft
add_executable(raft ${MAIN_RAFT})
target_link_libraries(raft pthread -lzmq)
## kv_raft
add_executable(kv_raft_server ${MAIN_KV_RAFT_SERVER})
target_link_libraries(kv_raft_server pthread -lzmq)
add_executable(kv_raft_client ${MAIN_KV_RAFT_CLIENT})
target_link_libraries(kv_raft_client pthread -lzmq)



# 设置安装的默认路径

# 将目录安装到 include/ryan_network 下（只安装头文件）
#install(DIRECTORY ${PROJECT_SOURCE_DIR}/ryan_network/ DESTINATION build/include/ryan_network FILES_MATCHING PATTERN "*.h")

# 加载example
#add_subdirectory(examples/echo_server)

#加载test
#add_subdirectory(ryan_network/base/test)
