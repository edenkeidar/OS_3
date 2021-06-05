//
// Created by mayam on 04-Jun-21.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <pthread.h>
#include <vector>
#include <string>
#include <iostream>
using namespace std;

// ------------------------------------------------------ declarations ---------------------------------------------
#define SYSTEM_ERROR "system error: "
#define THREAD_CREATE_ERROR "system couldn't create thread: "
#define FAIL 1
#define SUCCESS 0
struct Context{
    IntermediateVec* intermediary_elements;
    OutputVec* output_elements;
};

typedef struct{
    const InputVec *inputVec;
    OutputVec *outputVec;
    JobState state;
    pthread_t* threads = NULL;
    Context* contexts = NULL;
}Job;

void handle_error(int code, string message);
void map();
void sort();
void reduce();
void shuffle();
void wait_for_shuffle();
void* basic_thread_entry(void *);
void* main_thread_entry(void *);

// ------------------------------------------------------ API ---------------------------------------------

void emit2 (K2* key, V2* value, void* context){
    return;
}
void emit3 (K3* key, V3* value, void* context){
    return;
}



JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    Job* new_job;
    new_job->threads = new pthread_t[multiThreadLevel];
    new_job->contexts = new Context[multiThreadLevel];
    for (int i = 0; i < multiThreadLevel; ++i) {
        int err = pthread_create(&new_job.threads[i], NULL, &f, NULL);
        handle_error(err, THREAD_CREATE_ERROR);
        new_job->contexts[i].intermediary_elements =  new IntermediateVec;
        new_job->contexts[i].output_elements = new OutputVec;
    }
    return new_job;
}

void waitForJob(JobHandle job){
    return;
}

void getJobState(JobHandle job, JobState* state){
    Job* pointer = (Job*)job;
    *state = pointer->state;
}

void closeJobHandle(JobHandle job){
    return;
}


// ------------------------------------------------------ Auxiliary ---------------------------------------------


void handle_error(int code, string message){
    if (code){
        cout << SYSTEM_ERROR << message << "\n";
        exit(FAIL);
    }
}

void* basic_thread_entry(void *){
    map();
    sort();
    wait_for_shuffle();
    reduce();
}

void* f(void *){
    printf("!\n");
    return nullptr;
}
void* main_thread_entry(void *){
    map();
    sort();
    shuffle();
    reduce();
}