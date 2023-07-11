#ifndef RYAN_DS_KV_RAFT_SEMAPHORE_H_
#define RYAN_DS_KV_RAFT_SEMAPHORE_H_

#include <semaphore.h>

class Semaphore{
public:
    Semaphore(){
        if(sem_init(&m_sem, 0 ,0) != 0){
            throw std::exception();
        }
    }
    void Init(int num){
        if(sem_init(&m_sem, 0, num) != 0){
            throw std::exception();
        }
    }
    ~Semaphore(){
        sem_destroy(&m_sem);
    }
    bool Wait(){
        return sem_wait(&m_sem) == 0;
    }
    bool Timewait(struct timespec timeout){
        int ret = 0;    
        ret = sem_timedwait(&m_sem, &timeout);
        return ret == 0;
    }
    bool Post(){
        return sem_post(&m_sem) == 0;
    }
private:
    sem_t m_sem;
};

#endif  // RYAN_DS_KV_RAFT_SEMAPHORE_H_