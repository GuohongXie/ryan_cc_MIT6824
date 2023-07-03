#include "coordinator.h"
#include "buttonrpc.hpp"
constexpr int RPC_SERVER_PORT = 5555;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("missing parameter! The format is ./coordinator pg*.txt");
    exit(-1);
  }
  // alarm(10);
  buttonrpc coordinator_server;
  coordinator_server.as_server(RPC_SERVER_PORT);
  Coordinator coordinator(13, 9);  //map_num, reduce_num
  coordinator.GetAllFile(argc, argv);
  coordinator_server.bind("map_num", &Coordinator::map_num, &coordinator);
  coordinator_server.bind("reduce_num", &Coordinator::reduce_num, &coordinator);
  coordinator_server.bind("AssignTask", &Coordinator::AssignTask, &coordinator);
  coordinator_server.bind("SetMapStat", &Coordinator::SetMapStat, &coordinator);
  coordinator_server.bind("IsMapDone", &Coordinator::IsMapDone, &coordinator);
  coordinator_server.bind("AssignReduceTask", &Coordinator::AssignReduceTask, &coordinator);
  coordinator_server.bind("SetReduceStat", &Coordinator::SetReduceStat, &coordinator);
  coordinator_server.bind("Done", &Coordinator::Done, &coordinator);
  coordinator_server.run();
  return 0;
}