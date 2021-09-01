#include "uthreads.h"
#include "Thread.hpp"
#include "map"
#include <deque>
#include "algorithm"
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <iostream>

#define SUCCESS 0
#define FAIL -1
#define MUTEX_UNLOCK -1
#define MAIN_TID 0
// ERRORS:
#define E_TLIB_SPAWN "thread library error: cannot add new thread due to lack of space"
#define E_TLIB_SIGACTION "thread library error: SIGACTION error in init "
#define E_TLIB_NON_POS_INT "thread library error: non-positive integer."
#define E_TLIB_TID_DOESNT_EXIST "thread library error: given tid does not exist."
#define E_TLIB_MAIN_BLOCK "thread library error: main thread was blocked."
#define E_TLIB_SELF_MUTEX_BLOCK "thread library error: Thread is attempting to mutex-block itself, although currently has the mutex"
#define E_TLIB_SETITIMER "thread library error: settimer error"
#define E_TLIB_UNLOCK_UNLOCKED_MUTEX "thread library error: mutex already unlocked"
#define E_TLIB_MAIN_TERMINATE "thread library error: main thread was attempted to be terminated."
#define E_TLIB_UNLOCK_MUTEX_BY_DIFFERENT_THREAD "thread library error: mutex attempted to be unlocked by a different thread than the one that locked it"


static sigset_t set;
static sigset_t anotherSet;
static struct sigaction sigact;
struct itimerval timer;

void mutexUnlockHelper();

int findNextRunningThreadID(int nextThreadId);

static int quantum;
static int runningThread;
static std::map<int, class Thread> allThreadsMap;
static std::deque<int> mutexBlockedDeque;
static std::deque<int> readyList;
static int quantumSum;
static bool isBlocked = false;
/**
 * -1 if no one is using the mutex, other, tid of the thread holding the mutex (must be a non-neg int)
 */
static int mutex = -1;

static void blockSignal() {
    sigemptyset(&anotherSet);
    sigaddset(&anotherSet, SIGVTALRM);
    sigprocmask(SIG_BLOCK, &anotherSet, nullptr);
    isBlocked = true;
}

static void unblockSignal() {
    sigemptyset(&anotherSet);
    sigaddset(&anotherSet, SIGVTALRM);
    sigprocmask(SIG_UNBLOCK, &anotherSet, nullptr);
    isBlocked = false;
}

static void setTimer() {

    // Configure the timer to expire after 1 sec... */
    timer.it_value.tv_sec = 0;              // first time interval, seconds part
    timer.it_value.tv_usec = quantum;       // first time interval, microseconds part

    // configure the timer to expire every 3 sec after that.
    timer.it_interval.tv_sec = 0;           // following time intervals, seconds part
    timer.it_interval.tv_usec = 0;          // following time intervals, microseconds part

    // Start a virtual timer. It counts down whenever this process is executing.
    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
        std::cerr << E_TLIB_SETITIMER << std::endl;
        //exitProgram:
       exit(1);
    }
}

/**
 * handels sigvtalarm. implements round robin algorithm
 * @param signal
 */
static void signalManager(int signal) {
    // readylist empty - main thread is the only thread running
    if (readyList.empty()) {
        allThreadsMap[runningThread].increaseQuantum();
        quantumSum++;
        setTimer();
        unblockSignal();
        return;
    }
    // ready list is not empty

    // if running is not terminated - save the running env (if teminated, we don't need to)
    if (allThreadsMap[runningThread].getState() != TERMINATED) {
        int val = sigsetjmp(allThreadsMap[runningThread].getEnv(), 1);
        if (val != 0) {
            // it is a "fake return"
            // so now we need to delete all the threads with state terminate
            for (unsigned int i = 0; i < allThreadsMap.size(); i++) {
                if(allThreadsMap.count(i))
                {
                    if (allThreadsMap[i].getState()==TERMINATED) {
                        allThreadsMap.erase(i);
                    }
                }
            }
            setTimer();
            unblockSignal();
            return;
        }
    }
    // tried to lock the mutex, and couldn't so called the manager
    if (allThreadsMap[runningThread].getIsMutexBlocked()){
        //Move running thread to mutexblocked
        mutexBlockedDeque.push_front(runningThread);
        allThreadsMap[runningThread].setState(READY);
    }

    else if (allThreadsMap[runningThread].getState() == RUNNING) { //Move running thread to READY
        readyList.push_front(runningThread);
        allThreadsMap[runningThread].setState(READY);
    }
    // get the next thread we want to run:

    // find next thread id
    int nextThreadId = readyList.back();
    nextThreadId = findNextRunningThreadID(nextThreadId);

    // assign new running:
    runningThread = nextThreadId;
    allThreadsMap[runningThread].setState(RUNNING);
    allThreadsMap[runningThread].increaseQuantum();
    quantumSum++;

    setTimer(); // reset the clock
    unblockSignal();
    siglongjmp(allThreadsMap[runningThread].getEnv(), 1); //jump to the next running thread
}

int findNextRunningThreadID(int nextThreadId) {
    int readyListSize = readyList.size();
    for (int i = 0; i <readyListSize ; i++) {
        nextThreadId = readyList.back(); // redandant in i=0
        readyList.pop_back();
        // handle mutex
        // we want to check if we can move the nextThreadId to running state
        // if the next thread is waiting for mutex - it can only move to running if mutex is free
        if(allThreadsMap[nextThreadId].getIsMutexBlocked() ){
            if (mutex == MUTEX_UNLOCK ){
                // mutex is free - can move
                mutex = nextThreadId;
                allThreadsMap[nextThreadId].setIsMutexBlocked(false);
                // found the next thread Id!
                break;
            }
            else{
                // mutex is not free- next cannot move to running, so move back to mutex deque
                mutexBlockedDeque.push_front(nextThreadId);
            }

        }
        else {
            // nextThreadId does not requre the mutex - no problem moving it to running
            break;
        }
    }
    return nextThreadId;
}

/**
 * return a new valid tid upon success. -1 otherwise
 * @return non negative integer upon sucess, -1 otherwise
 */
int generateNewTid() {
    int i = 0;
    for (; i < (int) allThreadsMap.size(); i++) {
        if (!allThreadsMap.count(i) )
        {
            return i;
        }
        else if (allThreadsMap[i].getState()== TERMINATED){
            return i;
        }
    }
    if (i < MAX_THREAD_NUM) {
        return i;
    }
    return -1;
}


// ============================= API =========================================
/*
 * Description: This function initializes the thread library.
 * You may assume that this function is called before any other thread library
 * function, and that it is called exactly once. The input to the function is
 * the length of a quantum in micro-seconds. It is an error to call this
 * function with non-positive quantum_usecs.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_init(int quantum_usecs) {
    // On failure, return -1.
    if (quantum_usecs <= 0) {
        std::cerr << E_TLIB_NON_POS_INT << std::endl;
        return FAIL;
    }
    quantum = quantum_usecs;
    runningThread = 0;
    Thread main = Thread();
    allThreadsMap.insert(std::pair<int, class Thread>(runningThread,main));
    //  save sp,pc..
    allThreadsMap[runningThread].increaseQuantum();
    allThreadsMap[runningThread].setState(RUNNING);
    quantumSum++;

    // set signal
    sigemptyset(&set);
    sigaddset(&set, SIGVTALRM);
    sigact.sa_handler = &signalManager; // &signalManager??
    sigact.sa_flags = 0;
    if (sigaction(SIGVTALRM, &sigact, nullptr) < 0) { // then it's an error
        std::cerr << E_TLIB_SIGACTION << std::endl;
        exit(1);
    }

    //set timer
    setTimer();
    return SUCCESS;
}

/*
 * Description: This function creates a new thread, whose entry point is the
 * function f with the signature void f(void). The thread is added to the end
 * of the READY threads list. The uthread_spawn function should fail if it
 * would cause the number of concurrent threads to exceed the limit
 * (MAX_THREAD_NUM). Each thread should be allocated with a stack of size
 * STACK_SIZE bytes.
 * Return value: On success, return the ID of the created thread.
 * On failure, return -1.
*/
int uthread_spawn(void (*f)()) {
    // block signal so this functionality won't be interrupted
    blockSignal();
    // generate id
    int newTid = generateNewTid();
    if (newTid < 0) {
        std::cerr << E_TLIB_SPAWN << std::endl;
        unblockSignal();
        return FAIL;
    }
    // newTid is valid!
    // add to map a new thread with this tid
    allThreadsMap.insert(std::pair<int, class Thread>(newTid,Thread(f, newTid, STACK_SIZE)));

    readyList.push_front(newTid);
    // unlock and return tid
    unblockSignal();
    return newTid;
}


/*
 * Description: This function terminates the thread with ID tid and deletes
 * it from all relevant control structures. All the resources allocated by
 * the library for this thread should be released. If no thread with ID tid
 * exists it is considered an error. Terminating the main thread
 * (tid == 0) will result in the termination of the entire process using
 * exit(0) [after releasing the assigned library memory].
 * Return value: The function returns 0 if the thread was successfully
 * terminated and -1 otherwise. If a thread terminates itself or the main
 * thread is terminated, the function does not return.
*/
int uthread_terminate(int tid) {
    blockSignal();
    if (tid==MAIN_TID)
    {
        std::cerr << E_TLIB_MAIN_TERMINATE << std::endl;
        unblockSignal();
        exit(0);
    }

    if (allThreadsMap.find(tid) == allThreadsMap.end()) {
        // not found
        std::cerr << E_TLIB_TID_DOESNT_EXIST << std::endl;
        unblockSignal();
        return FAIL;
    }
    if (mutex == tid)
    {
        mutexUnlockHelper();
    }
    int state = allThreadsMap[tid].getState();
    if (allThreadsMap[tid].getIsMutexBlocked()) {
        int mutexBlockSize = mutexBlockedDeque.size();
        for (int i = 0; i < mutexBlockSize; i++) {
            if (mutexBlockedDeque[i] == tid) {
                mutexBlockedDeque.erase(mutexBlockedDeque.begin() + i - 1);
                allThreadsMap[tid].setState(TERMINATED);
                break;
            }
        }
    }
    else if (state == READY) {
        for (unsigned int i = 0; i < readyList.size(); i++) {
            if (readyList[i] == tid) {
                readyList.erase(readyList.begin() + i);
                allThreadsMap[tid].setState(TERMINATED);
                break;
            }
        }
    }
    else if (state == RUNNING) {
        allThreadsMap[runningThread].setState(TERMINATED);
        signalManager(1);
    }
    allThreadsMap.erase(tid);
    unblockSignal();
    return SUCCESS;
}


/*
 * Description: This function blocks the thread with ID tid. The thread may
 * be resumed later using uthread_resume. If no thread with ID tid exists it
 * is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision
 * should be made. Blocking a thread in BLOCKED state has no
 * effect and is not considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_block(int tid) {
    blockSignal();
    // On failure, return -1.
    if (!allThreadsMap.count(tid)) {
        std::cerr << E_TLIB_TID_DOESNT_EXIST << std::endl;
        unblockSignal();
        return -1;
    }
    // If it's the main thread then it's an error.
    if (tid == MAIN_TID) {
        std::cerr << E_TLIB_MAIN_BLOCK << std::endl;
        unblockSignal();
        return FAIL;
    }



    // If it's running thread that we need to block-
    if (tid == runningThread) {
        //change state to blocked , but keep it the running thread

        allThreadsMap[runningThread].setState(BLOCKED);

        //then, call (by reseting timer, or manual) signal manager
        signalManager(1);

        // sigvtalarm should be thrown, casuing signalHandler() to run.
        unblockSignal();
        return SUCCESS;
    }

    // if the thread in readylist -
    if (allThreadsMap[tid].getState() == READY) {
        for (unsigned int i = 0; i < readyList.size(); i++) {
            if (readyList[i] == tid) {
                readyList.erase(readyList.begin() + i);
                break;
            }
        }
        allThreadsMap[tid].setState(BLOCKED);
        unblockSignal();
        return SUCCESS;
    }

    // the thread is blocked - so it's not a mistake, do nothing
    // if mutex blocked
    if (allThreadsMap[tid].getIsMutexBlocked())
    {
        allThreadsMap[tid].setState(BLOCKED);
    }
    unblockSignal();
    return SUCCESS;
}

/*
 * Description: This function resumes a blocked thread with ID tid and moves
 * it to the READY state if it's not synced. Resuming a thread in a RUNNING or READY state
 * has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid) {
    blockSignal();
    if (allThreadsMap.find(tid) == allThreadsMap.end()) {
        // not found
        std::cerr << E_TLIB_TID_DOESNT_EXIST << std::endl;
        unblockSignal();
        return FAIL;
    }
    int state = allThreadsMap[tid].getState();

    if (state == BLOCKED) {
        if (!allThreadsMap[tid].getIsMutexBlocked())
        {
            readyList.push_front(tid);
        }
        allThreadsMap[tid].setState(READY);
    }
    unblockSignal();
    return SUCCESS;
}


/*
 * Description: This function tries to acquire a mutex.
 * If the mutex is unlocked, it locks it and returns.
 * If the mutex is already locked by different thread, the thread moves to BLOCK state.
 * In the future when this thread will be back to RUNNING state,
 * it will try again to acquire the mutex.
 * If the mutex is already locked by this thread, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_lock() {
    blockSignal();
    // If the mutex is already locked by this thread, it is considered an error.
    if (mutex == runningThread){
        std::cerr << E_TLIB_SELF_MUTEX_BLOCK << std::endl;
        unblockSignal();
        return FAIL;
    }

    // If the mutex is unlocked, it locks it and returns.
    if(mutex == MUTEX_UNLOCK){
        mutex = runningThread;

        unblockSignal();
        return SUCCESS;
    }
    //If the mutex is already locked by different thread, the thread moves to BLOCK state.
    // In the future when this thread will be back to RUNNING state,
    // It will try again to acquire the mutex.
    // if the mutex is already used - current thread get's in mutexBlockedDeque.
    if (mutex > -1 && mutex != runningThread) {
        // running into mutex blocked
        allThreadsMap[runningThread].setIsMutexBlocked(true);
        signalManager(1);
        unblockSignal();
        return SUCCESS;
    }
    return FAIL;
}


/*
 * Description: This function releases a mutex.
 * If there are blocked threads waiting for this mutex,
 * one of them (no matter which one) moves to READY state.
 * If the mutex is already unlocked, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_unlock() {
    blockSignal();
    if(mutex ==MUTEX_UNLOCK ){
        std::cerr << E_TLIB_UNLOCK_UNLOCKED_MUTEX << std::endl;
        unblockSignal();
        return FAIL;
    }
    else if (mutex==runningThread)
    {
        mutexUnlockHelper();
        unblockSignal();
        return SUCCESS;
    }
    else{
        std::cerr << E_TLIB_UNLOCK_MUTEX_BY_DIFFERENT_THREAD << std::endl;
        unblockSignal();
        return FAIL;
    }
}

void mutexUnlockHelper() {
    mutex = MUTEX_UNLOCK;
    int moveToReadylist;
    if (!mutexBlockedDeque.empty()){
        // there is at least one thread waiting to get the mutex.
        // make sure the thread we chose is not in Block state

        int mutexBlockSize = mutexBlockedDeque.size();
        for (int i = 0; i < mutexBlockSize; i++) {
            // check if it's ready
            if (allThreadsMap[mutexBlockedDeque.back()].getState() ==READY){
                //allThreadsMap[mutexBlockedDeque.back()].setIsMutexBlocked(false);
                moveToReadylist = mutexBlockedDeque.back();
                mutexBlockedDeque.pop_back();
                // if we found a thread in MutexblockDeque which is currently in ready state - move to ready list
                readyList.push_front(moveToReadylist);
                break;
            }
            int moveToFront = mutexBlockedDeque.back();
            mutexBlockedDeque.pop_back();
            mutexBlockedDeque.push_front(moveToFront);
        }
    }
}


/*
 * Description: This function returns the thread ID of the calling thread.
 * Return value: The ID of the calling thread.
*/
int uthread_get_tid() {
    return runningThread;
}


/*
 * Description: This function returns the total number of quantums since
 * the library was initialized, including the current quantum.
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number
 * should be increased by 1.
 * Return value: The total number of quantums.
*/
int uthread_get_total_quantums() {
    return quantumSum;
}

/*
 * Description: This function returns the number of quantums the thread with
 * ID tid was in RUNNING state. On the first time a thread runs, the function
 * should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state
 * when this function is called, include also the current quantum). If no
 * thread with ID tid exists it is considered an error.
 * Return value: On success, return the number of quantums of the thread with ID tid.
 * 			     On failure, return -1.
*/
int uthread_get_quantums(int tid) {
    blockSignal();
    if (allThreadsMap.find(tid) == allThreadsMap.end()) {
        // not found
        std::cerr << E_TLIB_TID_DOESNT_EXIST << std::endl;
        unblockSignal();
        return FAIL;
    }
    int temp = allThreadsMap[tid].getQuantums();
    unblockSignal();
    return temp;
}

