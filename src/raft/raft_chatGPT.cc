#include <chrono>
#include <thread>

void Raft::SetBroadcastTime() {
  last_broadcast_time_ = std::chrono::system_clock::now();
  last_broadcast_time_ -= std::chrono::microseconds(200000);
}

void Raft::ListenForVote() {
  buttonrpc server;
  server.as_server(peers_[peer_id_].port.first);
  server.bind("RequestVote", &Raft::RequestVote, this);

  std::thread electionThread(&Raft::ElectionLoop, this);
  electionThread.detach();

  server.run();
  printf("exit!\n");
}

void Raft::ListenForAppend() {
  buttonrpc server;
  server.as_server(peers_[peer_id_].port.second);
  server.bind("AppendEntries", &Raft::AppendEntries, this);

  std::thread processEntriesThread(&Raft::ProcessEntriesLoop, this);
  processEntriesThread.detach();

  server.run();
  printf("exit!\n");
}

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

void Raft::ElectionLoop(void* arg) {
  Raft* raft = static_cast<Raft*>(arg);
  bool resetFlag = false;
  while (!raft->is_dead_) {
    int timeOut = rand() % 200000 + 200000;
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      std::lock_guard<std::mutex> lock(raft->m_lock);
      int during_time = raft->GetMyduration(raft->last_wake_time_);
      if ((raft->state_ == FOLLOWER || raft->state_ == CANDIDATE) &&
          during_time > timeOut) {
        if (raft->state_ == FOLLOWER) {
          raft->state_ = CANDIDATE;
        } else {
          printf(" %d attempt election at term %d, timeOut is %d\n",
                 raft->peer_id_, raft->curr_term_, timeOut);
          gettimeofday(&raft->last_wake_time_, nullptr);
          resetFlag = true;
          raft->curr_term_++;
          raft->voted_for_ = raft->peer_id_;
          raft->SaveRaftState();

          raft->recv_votes_ = 1;
          raft->finished_vote_ = 1;
          raft->curr_peer_id_ = 0;

          std::vector<std::thread> threads;
          for (auto& server : raft->peers_) {
            if (server.peer_id_ == raft->peer_id_) continue;
            threads.emplace_back(CallRequestVote, raft);
          }

          for (auto& thread : threads) {
            if (thread.joinable()) {
              thread.detach();
            }
          }

          while (raft->recv_votes_ <= raft->peers_.size() / 2 &&
                 raft->finished_vote_ != raft->peers_.size()) {
            raft->m_cond.wait(lock);
          }

          if (raft->state_ != CANDIDATE) {
            continue;
          }

          if (raft->recv_votes_ > raft->peers_.size() / 2) {
            raft->state_ = LEADER;
            for (int i = 0; i < raft->peers_.size(); i++) {
              raft->next_index_[i] = raft->logs_.size() + 1;
              raft->match_index_[i] = 0;
            }

            printf(" %d become new leader at term %d\n", raft->peer_id_,
                   raft->curr_term_);
            raft->SetBroadcastTime();
          }
        }
      }
      if (resetFlag) {
        resetFlag = false;
        break;
      }
    }
  }
}
