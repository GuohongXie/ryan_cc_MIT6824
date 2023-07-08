# 指定使用 CMake 的最低版本号
cmake_minimum_required(VERSION 3.0)

# 设置项目名称
project(ryan_map_reduce C CXX)

# 添加搜索路径
include_directories(
    ${PROJECT_SOURCE_DIR}/../../src/map_reduce/mr
    ${PROJECT_SOURCE_DIR}/../../src/buttonrpc
    ${PROJECT_SOURCE_DIR}/../../src/buttonrpc/depends
    )


# 包含这些目录下的.cc文件
# 下面这两个会把目录及其子目录的所有.cc文件都包括
#aux_source_directory(${PROJECT_SOURCE_DIR}/ryan_network/logger SRC_LOGGER)
#aux_source_directory(${PROJECT_SOURCE_DIR}/ryan_network/logger SRC_LOGGER)
#如果想要不包含子目录下的文件，则使用file
file(GLOB CC_COORDINATOR ${PROJECT_SOURCE_DIR}/../../src/map_reduce/mr/coordinator.cc)
file(GLOB CC_WORKER ${PROJECT_SOURCE_DIR}/../../src/map_reduce/mr/worker.cc)
file(GLOB MAIN_COORDINATOR ${PROJECT_SOURCE_DIR}/../../src/map_reduce/main/mrcoordinator.cc)
file(GLOB MAIN_WORKER ${PROJECT_SOURCE_DIR}/../../src/map_reduce/main/mrworker.cc)
file(GLOB MAIN_WORKER_OLD ${PROJECT_SOURCE_DIR}/../../src/map_reduce/main/mrworker_old.cc)

# 设置编译选项
set(CXX_FLAGS
    -g
    -Wall
    -std=c++11
    -O0
    )

#find_package(Boost REQUIRED)
#find_library(BOOST_TEST_LIBRARY NAMES boost_unit_test_framework)
#include_directories(${Boost_INCLUDE_DIRS})

add_executable(mrcoordinator ${MAIN_COORDINATOR} ${CC_COORDINATOR})
target_link_libraries(mrcoordinator pthread -lzmq)

add_executable(mrworker ${MAIN_WORKER})
target_link_libraries(mrworker pthread -ldl -lzmq)

add_executable(mrworker_old ${MAIN_WORKER_OLD})
target_link_libraries(mrworker_old pthread -ldl -lzmq)


set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
#set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)


# 设置安装的默认路径

# 将目录安装到 include/ryan_network 下（只安装头文件）
#install(DIRECTORY ${PROJECT_SOURCE_DIR}/ryan_network/ DESTINATION build/include/ryan_network FILES_MATCHING PATTERN "*.h")

# 加载example
#add_subdirectory(examples/echo_server)

#加载test
#add_subdirectory(ryan_network/base/test)
