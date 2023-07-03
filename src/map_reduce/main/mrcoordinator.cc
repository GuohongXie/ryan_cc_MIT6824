#include "coordinator.h"
#include "buttonrpc.hpp"
constexpr int RPC_SERVER_PORT = 5555;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("missing parameter! The format is ./coordinator pg*.txt");
    exit(-1);
  }
  // alarm(10);
  buttonrpc server;
  server.as_server(RPC_SERVER_PORT);
  Coordinator coordinator(13, 9);  //map_num, reduce_num
  coordinator.GetAllFile(argc, argv);
  server.bind("map_num", &Coordinator::map_num, &coordinator);
  server.bind("reduce_num", &Coordinator::reduce_num, &coordinator);
  server.bind("AssignTask", &Coordinator::AssignTask, &coordinator);
  server.bind("SetMapStat", &Coordinator::SetMapStat, &coordinator);
  server.bind("IsMapDone", &Coordinator::IsMapDone, &coordinator);
  server.bind("AssignReduceTask", &Coordinator::AssignReduceTask, &coordinator);
  server.bind("SetReduceStat", &Coordinator::SetReduceStat, &coordinator);
  server.bind("Done", &Coordinator::Done, &coordinator);
  server.run();
  return 0;
}