#!/bin/bash
set -x

# delete old executables
rm src/map_reduce/main/libmr_word_count.so
rm src/map_reduce/main/mrworker
rm src/map_reduce/main/mrcoordinator
rm src/raft/raft
rm src/kv_raft/kv_raft_server
rm src/kv_raft/kv_raft_client
rm src/shard_kv/shard_master/shard_kv_server_a
rm src/shard_kv/shard_master/shard_kv_client_a
rm src/shard_kv/shard_master/shard_kv_test_a
rm src/shard_kv/shard_kv_server_b
rm src/shard_kv/shard_kv_client_b
rm src/shard_kv/shard_kv_test_b
rm src/buttonrpc/example/buttonrpc_server
rm src/buttonrpc/example/buttonrpc_client

# delete old build files
rm -rf build
rm compile_commands.json

# build
mkdir build
cd ./build
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_CXX_COMPILER=clang++ ..
cp compile_commands.json ../

make

# copy executables
cp bin/mrworker ../src/map_reduce/main/
cp bin/mrcoordinator ../src/map_reduce/main/
cp lib/libmr_word_count.so ../src/map_reduce/mrapps/
cp bin/raft ../src/raft/
cp bin/kv_raft_server ../src/kv_raft/
cp bin/kv_raft_client ../src/kv_raft/
cp bin/shard_kv_server_a ../src/shard_kv/shard_master/
cp bin/shard_kv_client_a ../src/shard_kv/shard_master/
cp bin/shard_kv_test_a ../src/shard_kv/shard_master/
cp bin/shard_kv_server_b ../src/shard_kv/
cp bin/shard_kv_client_b ../src/shard_kv/
cp bin/shard_kv_test_b ../src/shard_kv/
cp bin/buttonrpc_server ../src/buttonrpc/example/
cp bin/buttonrpc_client ../src/buttonrpc/example/

# remove middle files (OPTIONAL)
rm ../src/kv_raft/persister*
rm ../src/kv_raft/snap_shot*
rm -f ../src/kv_raft/fifo-*
rm ../src/raft/persister*
rm ../src/shard_kv/shard_master/persister*
rm ../src/shard_kv/persister*
