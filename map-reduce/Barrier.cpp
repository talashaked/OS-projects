#include "Barrier.h"
#include <cstdlib>
#include <cstdio>

Barrier::Barrier(int numThreads)
		: mutex(PTHREAD_MUTEX_INITIALIZER)
		, cv(PTHREAD_COND_INITIALIZER)
		, count(0)
		, numThreads(numThreads)
{ }


Barrier::~Barrier()
{
	if (pthread_mutex_destroy(&mutex) != 0) {
		fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
		exit(1);
	}
	if (pthread_cond_destroy(&cv) != 0){
		fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
		exit(1);
	}
}


void Barrier::barrier()
{
    //lock mutex
	if (pthread_mutex_lock(&mutex) != 0){
		fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
		exit(1);
	}
	// if num of threads which did barrier "up" < numthread
	if (++count < numThreads) {
            if (pthread_cond_wait(&cv, &mutex) != 0){
                fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
                exit(1);
            }
	} else {
            count = 0;
            if (pthread_cond_broadcast(&cv) != 0) {
                fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
                exit(1);
            }
	}
	if (pthread_mutex_unlock(&mutex) != 0) {
		fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
		exit(1);
	}
}
