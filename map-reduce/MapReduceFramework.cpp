
// =======================includes=======================
#include <atomic>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <pthread.h>
#include <algorithm>
#include <map>
// =======================defines=======================
#define ERR_PTHREAD_CREATE "system error: pthread_create failed."
#define ERR_MUTEX_INIT "system error: mutexInit failed."
#define ERR_MUTEX_LOCK "system error: mutexLock failed."
#define ERR_MUTEX_UNLOCK "system error: mutexUnlock failed."
#define ERR_MUTEX_DESTROY "system error: killmutex failed."
#define ERR_PTHREAD_JOIN "system error: join thread failed."
// =======================STRUCTS=======================

struct K2PointerComp
{
    bool operator()(const K2 *first, const K2 *second) const
    {
        return ((*first) < (*second));
    }
};

typedef std::map<K2 *, IntermediateVec, K2PointerComp> IntermediateMap;

/**
 * represents the thread's context.
 */
struct ThreadContext
{

    int threadID;
    Barrier *barrier;
    const InputVec *inputVec;
    OutputVec *outputVec;
    std::vector<IntermediateVec> *afterShuffleVecofVecs;
    std::atomic<int> *state;
    std::atomic<int> *totalNumOfPairs;// after map phase
    int multiThreadLevel;
    std::atomic<int> *StartedCounter;
    std::atomic<int> *EndedCounter;
    const MapReduceClient *client;
    pthread_mutex_t *precentMutex;
    pthread_mutex_t *waitMutex;
    pthread_mutex_t *intermediateVecMutex;
    pthread_mutex_t *outputMutex;
    IntermediateVec *intermediateVecArr;

};


/**
 * represent a job. holds the threads, their context with their state/status.
 */
struct Job
{
    // maybe should recive thread wrapper
    Job(int multiThreadLevel, ThreadContext *tcontexts, pthread_t *threads) :
            numThreads(multiThreadLevel), tContexts(tcontexts), threads(threads), wait(false)
    {
    }

    ThreadContext *tContexts;
    int numThreads; //- size of the DS above
    pthread_t *threads;
    bool wait;

};


// =======================FUNCTIONS =======================
// =======================helper functions=======================
void mutexUnlock(pthread_mutex_t *mutex)
{
    if (pthread_mutex_unlock(mutex))
    {
        std::cerr << ERR_MUTEX_UNLOCK << std::endl;
        exit(1);
    }
}

void mutexLock(pthread_mutex_t *mutex)
{
    if (pthread_mutex_lock(mutex))
    {
        std::cerr << ERR_MUTEX_LOCK << std::endl;
        exit(1);
    }
}

void mutexInitiate(pthread_mutex_t *mutex) {
    if (pthread_mutex_init(mutex, nullptr))
    {
        std::cerr << ERR_MUTEX_INIT << std::endl;
        exit(1);
    }
}

void mutexDestroy(pthread_mutex_t *mutex) {
    if (pthread_mutex_destroy(mutex))
    {
        std::cerr << ERR_MUTEX_DESTROY << std::endl;
        exit(1);
    }
}


// Compares two pairs according to K2 vals.
bool compareK2(IntermediatePair pair1, IntermediatePair pair2)
{
    return (*(pair1.first)) < (*(pair2.first));
}

/**
 * reduce input to outputVec
 * @param tcontext Context of a single thread
 */
void activateReduce(ThreadContext *tcontext)
{
    int curVecInd = (*(tcontext->StartedCounter))++;
    while (curVecInd < (int)tcontext->afterShuffleVecofVecs->size())
    {
        IntermediateVec* curInterVec = &tcontext->afterShuffleVecofVecs->at(curVecInd);
        tcontext->client->reduce(curInterVec, tcontext);
        pthread_mutex_lock(tcontext->precentMutex);
        (*(tcontext->EndedCounter))+=curInterVec->size();
        pthread_mutex_unlock(tcontext->precentMutex);
        curVecInd = (*(tcontext->StartedCounter))++;
    }
}


void threadDoShuffle(ThreadContext *tcontext) {//updating counter before moving into shuffle stage
    pthread_mutex_lock(tcontext->precentMutex);
    (*(tcontext->StartedCounter)) = 0;
    (*(tcontext->state)) = SHUFFLE_STAGE; //update stage to shuffle
    (*(tcontext->EndedCounter)) = 0;
    pthread_mutex_unlock(tcontext->precentMutex);
    // do shuffle
    // create map [key, vector of [key,values] with same key
    // put all pairs in map
    while ((tcontext->EndedCounter->load()) < tcontext->totalNumOfPairs->load())
    {
        //first we need to find the biggest K2 to compare all K2 to
        IntermediatePair *curPair = &tcontext->intermediateVecArr[0].back();
        //iterate over the sorted array to find the biggest last K2
        for (int i = 0; i < tcontext->multiThreadLevel; ++i) {
            if (curPair == nullptr)
            {
                curPair = &tcontext->intermediateVecArr[i].back();
            }
            else if (tcontext->intermediateVecArr[i].empty())
            {
                continue;
            }
            else if (*(curPair->first) < *(tcontext->intermediateVecArr[i].back().first)) {
                curPair = &tcontext->intermediateVecArr[i].back();
            }
        }
        // create new IntermediateVec to push afterwards
        IntermediateVec newVec;
        //now add all the curPair K2 pairs to the new vec
        for (int i = 0; i < tcontext->multiThreadLevel; ++i)
        {
            if (tcontext->intermediateVecArr[i].empty()) {
                continue;
            }
            while ((!tcontext->intermediateVecArr[i].empty())&&((!(*(curPair->first) < *(tcontext->intermediateVecArr[i].back().first))) &&
                   (!(*(tcontext->intermediateVecArr[i].back().first) < *(curPair->first)))))
                   {
                newVec.push_back(tcontext->intermediateVecArr[i].back());
                tcontext->intermediateVecArr[i].pop_back();
                pthread_mutex_lock(tcontext->precentMutex);
                (*tcontext->EndedCounter)++;
                pthread_mutex_unlock(tcontext->precentMutex);
            }
        }
        //lastly, push the curPair K2 vec to the afterShufflevecOfVecs
        tcontext->afterShuffleVecofVecs->push_back(newVec);
    }

    //updating counter before moving into reduce stage
    pthread_mutex_lock(tcontext->precentMutex);
    (*(tcontext->StartedCounter)) = 0;
    (*(tcontext->state)) = REDUCE_STAGE; //update stage to reduce
    (*(tcontext->EndedCounter)) = 0;
    pthread_mutex_unlock(tcontext->precentMutex);

}

void threadDoMap(ThreadContext *tcontext) {
    (*(tcontext->state)) = MAP_STAGE; //update stage to map
    int curElemIdx = (*(tcontext->StartedCounter))++;
    //check if index in range - if it is then send it to client map
    while (curElemIdx < (int)tcontext->inputVec->size())
    {
        // get key, value and run client map on them:
        K1 *key = tcontext->inputVec->at(curElemIdx).first;
        V1 *value = tcontext->inputVec->at(curElemIdx).second;

        tcontext->client->map(tcontext->inputVec->at(curElemIdx).first, tcontext->inputVec->at(curElemIdx).second, tcontext);
        //sorting with a compatible comparator
        pthread_mutex_lock(tcontext->precentMutex);
        (*(tcontext->EndedCounter))++;
        pthread_mutex_unlock(tcontext->precentMutex);
        //try to work on the next element
        curElemIdx = (*(tcontext->StartedCounter))++;
    }
    std::sort(tcontext->intermediateVecArr[tcontext->threadID].begin(),
              tcontext->intermediateVecArr[tcontext->threadID].end(), compareK2);
}


/**
 * this function is called upon pothread_create
 * a thread calls this functions to know what to do. all threads call this function
 * @param pointer of a tcontext
 * @return nullptr by definition (for pthread_create
 */
void *threadDoJob(void *pointer)
{
    auto *tcontext = (ThreadContext *) pointer;
    //  ---------------------  3 stages: ---------------------

    //  --  Map --
    threadDoMap(tcontext);


    //  --  barrier --
    // wait in barrier for map then shuffle to be done with:
    tcontext->barrier->barrier();

    //  --  shuffle --
    if (tcontext->threadID == 0)
    {
        threadDoShuffle(tcontext);
    }

    //  --  barrier --
    tcontext->barrier->barrier();

    //  --  reduce --
    activateReduce(tcontext);

    return nullptr;
}



/**
 * initilazing job and all it's theads contexts
 * @param multiThreadLevel
 * @param inputVec
 * @param outputVec
 * @param client
 * @param threads
 * @return JobHandle of type Job
 */
void initializeJob(int multiThreadLevel, const InputVec &inputVec,
                        OutputVec &outputVec, const MapReduceClient &client,
                        ThreadContext *tContexts )
{
    //create objects
    auto EndedCounter = new std::atomic<int>(0);
    auto StartedCounter = new std::atomic<int>(0);
    auto totalNumOfPairs = new std::atomic<int>(0);
    auto barrier = new Barrier(multiThreadLevel);
    auto state = new std::atomic<int>(0);
    auto intermediateVecArray = new IntermediateVec[multiThreadLevel];
    auto afterShuffleVecofVecs = new std::vector<IntermediateVec>;

    auto outputMutex = new pthread_mutex_t;
    mutexInitiate(outputMutex);

    auto mapStageMutex = new pthread_mutex_t;
    mutexInitiate(mapStageMutex);

    auto waitMutexinit = new pthread_mutex_t;
    mutexInitiate(waitMutexinit);

    auto interVecMutex = new pthread_mutex_t;
    mutexInitiate(interVecMutex);

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        tContexts[i].threadID = i;
        tContexts[i].inputVec = &inputVec;
        tContexts[i].outputVec = &outputVec;
        tContexts[i].client = &client;
        tContexts[i].intermediateVecArr = intermediateVecArray;
        tContexts[i].afterShuffleVecofVecs = afterShuffleVecofVecs;
        tContexts[i].outputMutex = outputMutex;
        tContexts[i].precentMutex = mapStageMutex;
        tContexts[i].waitMutex = waitMutexinit;
        tContexts[i].barrier = barrier;
        tContexts[i].intermediateVecMutex = interVecMutex;
        tContexts[i].state = state;
        tContexts[i].StartedCounter = StartedCounter;
        tContexts[i].EndedCounter = EndedCounter;
        tContexts[i].totalNumOfPairs = totalNumOfPairs;
        tContexts[i].multiThreadLevel = multiThreadLevel;
    }

}

// ======================= api implementation=======================

void emit2(K2 *key, V2 *value, void *context)
{
    int tId = ((ThreadContext *) context)->threadID;
    mutexLock(((ThreadContext *) context)->intermediateVecMutex );
    ((ThreadContext *) context)->intermediateVecArr[tId].emplace_back(key, value);
    (*((ThreadContext *) context)->totalNumOfPairs)++;
    mutexUnlock(((ThreadContext *) context)->intermediateVecMutex);
}

void emit3(K3 *key, V3 *value, void *context)
{
    mutexLock(((ThreadContext *) context)->outputMutex);
    ((ThreadContext *) context)->outputVec->emplace_back(key, value);
    mutexUnlock(((ThreadContext *) context)->outputMutex);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{

    auto *threads = new pthread_t[multiThreadLevel];

    // create thread context and fill them up:
    auto *tContexts = new ThreadContext[multiThreadLevel];

    // create job with context for each thread
    initializeJob(multiThreadLevel, inputVec, outputVec, client, tContexts);

    Job* job = new Job(multiThreadLevel, tContexts, threads);

    // initialize stage: map
    (*((Job *) job)->tContexts[0].state) = MAP_STAGE;

    // create threads
    for (int i = 0; i < multiThreadLevel; i++)
    {

        if (pthread_create(threads + i,
                           nullptr,
                           threadDoJob,
                           tContexts + i))
        {
            std::cerr << ERR_PTHREAD_CREATE << std::endl;
            delete [] threads;
            threads = nullptr;
            // todo - ERASE TCONTEXT
            exit(1);
        }
    }
    // return jobHandler
    return static_cast<Job*>(job);//todo -change

}

void waitForJob(JobHandle job)
{

    if (job == nullptr)
    {
        return;
    }
    auto tcontext = (ThreadContext)((Job *) job)->tContexts[0];
    pthread_mutex_lock(tcontext.waitMutex);

    if(((Job *) job)->threads == nullptr){
        return;
    }
    if (!((Job*)job)->wait)
    {
        ((Job*)job)->wait = true;
        Job* curJob = (Job*)job;
        for (int i=0 ; i<curJob->numThreads;i++)
        {
            if(pthread_join(curJob->threads[i], nullptr))
            {
                std::cerr << ERR_PTHREAD_JOIN << std::endl;
                exit(1);
            }
        }

        delete[] ((Job *) job)->threads;
        ((Job *) job)->threads = nullptr;
    }
    pthread_mutex_unlock(tcontext.waitMutex);

}

void getJobState(JobHandle job, JobState *state)
{
    auto tcontext = (ThreadContext)((Job *) job)->tContexts[0];
    pthread_mutex_lock(tcontext.precentMutex);
    int curState = tcontext.state->load();

    if (curState == UNDEFINED_STAGE)
    {
        state->stage = UNDEFINED_STAGE;
        state->percentage = 100;
    }
    else if (curState == MAP_STAGE)
    {

        state->stage = MAP_STAGE;
        int vecInputSize = ((Job *) job)->tContexts[0].inputVec->size();
        int counterEndedMap = ((Job *) job)->tContexts[0].EndedCounter->load();
        state->percentage = (float) (counterEndedMap * 100) / (float) vecInputSize;

    }

    else if (curState == SHUFFLE_STAGE)
    {
        state->stage = SHUFFLE_STAGE;
        int vecInputSize = *(((Job *) job)->tContexts[0].totalNumOfPairs);
        int counterEndedMap = ((Job *) job)->tContexts[0].EndedCounter->load();
        state->percentage = (float) (counterEndedMap * 100) / (float) vecInputSize;
    }

    else if (curState == REDUCE_STAGE)
    {

        state->stage = REDUCE_STAGE;
        int vecInputSize = *(((Job *) job)->tContexts[0].totalNumOfPairs);
        int counterEndedMap = ((Job *) job)->tContexts[0].EndedCounter->load();
        state->percentage = (float) (counterEndedMap * 100) / (float) vecInputSize;

    }
    pthread_mutex_unlock(tcontext.precentMutex);
}

void closeJobHandle(JobHandle job)
{

    if (job == nullptr)
    {
        return;
    }
    waitForJob(job);
//    ((Job *) job)->numThreads = 0;
    ((Job *) job)->wait = false;
    auto tcontext = (ThreadContext)((Job *) job)->tContexts[0];
    // delete
    delete ((Job *) job)->tContexts[0].EndedCounter;
    ((Job *) job)->tContexts[0].EndedCounter = nullptr;

    delete ((Job *) job)->tContexts[0].StartedCounter;
    ((Job *) job)->tContexts[0].StartedCounter = nullptr;

    delete ((Job *) job)->tContexts[0].totalNumOfPairs;
    ((Job *) job)->tContexts[0].totalNumOfPairs = nullptr;

    delete ((Job *) job)->tContexts[0].barrier;
    ((Job *) job)->tContexts[0].barrier = nullptr;

    delete ((Job *) job)->tContexts[0].state;
    ((Job *) job)->tContexts[0].state = nullptr;

    delete[] ((Job *) job)->tContexts[0].intermediateVecArr;
    ((Job *) job)->tContexts[0].intermediateVecArr = nullptr;

    delete ((Job *) job)->tContexts[0].afterShuffleVecofVecs; //??
    ((Job *) job)->tContexts[0].afterShuffleVecofVecs = nullptr;

    // delete mutex

    mutexDestroy(((Job *) job)->tContexts[0].intermediateVecMutex);
    delete ((Job *) job)->tContexts[0].intermediateVecMutex;
    ((Job *) job)->tContexts[0].intermediateVecMutex = nullptr;

    mutexDestroy(((Job *) job)->tContexts[0].outputMutex);
    delete ((Job *) job)->tContexts[0].outputMutex;
    ((Job *) job)->tContexts[0].outputMutex = nullptr;

    mutexDestroy(((Job *) job)->tContexts[0].precentMutex);
    delete ((Job *) job)->tContexts[0].precentMutex;
    ((Job *) job)->tContexts[0].precentMutex = nullptr;


//    mutexDestroy(((Job *) job)->tContexts[0].waitMutex);
    delete ((Job *) job)->tContexts[0].waitMutex;
    ((Job *) job)->tContexts[0].waitMutex = nullptr;



    delete[] ((Job *) job)->tContexts;
    ((Job *) job)->tContexts = nullptr;

    delete static_cast<Job*>(job);


}