#include <cstdio>
#include <cstdlib>

#include "buttonrpc/buttonrpc.hpp"
#include "map_reduce/mr/coordinator.h"

constexpr int RPC_SERVER_PORT = 5555;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("missing parameter! The format is ./mrcoordinator pg*.txt");
    std::exit(-1);
  }
  // alarm(10);
  buttonrpc coordinator_server;
  coordinator_server.as_server(RPC_SERVER_PORT);
  Coordinator coordinator(8, 8);  // map_worker_num, reduce_worker_num
  coordinator.GetAllFile(argc, argv);
  coordinator_server.bind("map_worker_num", &Coordinator::map_worker_num,
                          &coordinator);
  coordinator_server.bind("reduce_worker_num", &Coordinator::reduce_worker_num,
                          &coordinator);
  coordinator_server.bind("AssignAMapTask", &Coordinator::AssignAMapTask,
                          &coordinator);
  coordinator_server.bind("SetAMapTaskFinished",
                          &Coordinator::SetAMapTaskFinished, &coordinator);
  coordinator_server.bind("IsMapDone", &Coordinator::IsMapDone, &coordinator);
  coordinator_server.bind("AssignAReduceTask", &Coordinator::AssignAReduceTask,
                          &coordinator);
  coordinator_server.bind("SetAReduceTaskFinished",
                          &Coordinator::SetAReduceTaskFinished, &coordinator);
  coordinator_server.bind("IsAllMapAndReduceDone",
                          &Coordinator::IsAllMapAndReduceDone, &coordinator);
  coordinator_server.run();
  return 0;
}