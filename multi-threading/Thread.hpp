//
// Created by talas on 5/8/2021.
//

#ifndef OS_EX2_WITHMA_THREADS_HPP
#define OS_EX2_WITHMA_THREADS_HPP

#include <stdio.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>

#define SECOND 1000000
#define STACK_SIZE 8192




// =================== processor address translator ======================
#ifdef __x86_64__

/* code for 64 bit Intel arch */

typedef unsigned long address_t;

#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
                 "rol    $0x11,%0\n"
    : "=g" (ret)
    : "0" (addr));
    return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr) {
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
                 "rol    $0x9,%0\n"
    : "=g" (ret)
    : "0" (addr));
    return ret;
}

#endif
// ========================================================================


typedef void (*StartFunc)();

enum States {
    READY, BLOCKED, RUNNING, TERMINATED
};

class Thread {
private:
    // notes:
    StartFunc _func;
    int _tid;
    int _stackSize;
    char *_stack; //todo check
    States _state; // state - READY,  RUNNING, BLOCKED
    int _quantumTCount;
    sigjmp_buf _env;
    bool isMutexBlocked;

public:
    // any thread which is not th main thread
    Thread(StartFunc func, int tid, int stackSize) : _func(func), _tid(tid), _stackSize(stackSize),
                                                     _stack(new char[stackSize]), _state(READY), _quantumTCount(0),
                                                     isMutexBlocked(false) {

        address_t sp, pc;

        sp = (address_t) _stack + _stackSize - sizeof(address_t);
        pc = (address_t) func;

        //todo : if (sigsetjmp(_env, 1) == 0); can it fail?
        if(sigsetjmp(_env, 1) == 0){
            (_env->__jmpbuf)[JB_SP] = translate_address(sp);
            (_env->__jmpbuf)[JB_PC] = translate_address(pc);
            sigemptyset(&_env->__saved_mask);
        }
        // todo - may all the func needs a try catch with error?
    }

    // constructor for main thread,
    Thread() : _func(nullptr), _tid(0), _stackSize(0), _stack(nullptr), _state(RUNNING), _quantumTCount(0),
               isMutexBlocked(false) {
        address_t sp, pc;

        sp = (address_t) _stack + _stackSize - sizeof(address_t);
        pc = (address_t) _func;

        //todo : if (sigsetjmp(_env, 1) == 0); can it fail?
        sigsetjmp(_env, 1) == 0;
        (_env->__jmpbuf)[JB_SP] = translate_address(sp);
        (_env->__jmpbuf)[JB_PC] = translate_address(pc);
        sigemptyset(&_env->__saved_mask);
        // todo - may all the func needs a try catch with error?
        // todo - check if need to do try and catch
        // todo - might need copy
    }

/**
 * Copy constructor.
 * @param other - the other thread to deep copy from.
 */
    Thread(const Thread &other) : Thread(other._func, other._tid,
                                         other._stackSize)
    {
    }

    bool operator==(const Thread &other)
    {
        return (this->_tid = other._tid &&
                this->_stack == other._stack &&
                             this->_stackSize == other._stackSize &&
                             this->_env == other._env &&
                             this->_quantumTCount == other._quantumTCount &&
                             this->_state == other._state &&
                             this->_func == other._func &&
                             this->isMutexBlocked == other.isMutexBlocked);
    }
    ~Thread() {
        if (_stack != nullptr)
        {
            delete[] _stack;
        }
        _stack = nullptr;
    }



    void increaseQuantum() {
        _quantumTCount++;
    }

    int getTid() {
        return _tid;
    }

    int getSize() {
        return _stackSize;
    }

//    char &getStack() {
//        return _stack; // todo in in sig might be &
//    }

    sigjmp_buf &getEnv() {
        return _env;
    }

    States getState() {
        return _state;
    }

    void setState(States newState) {
        _state = newState;
    }

    int getQuantums() {
        return _quantumTCount;
    }

    bool getIsMutexBlocked() {
        return isMutexBlocked;
    }

    void setIsMutexBlocked(bool val) {
        isMutexBlocked = val;
    }

};

#endif //OS_EX2_WITHMA_THREADS_HPP
