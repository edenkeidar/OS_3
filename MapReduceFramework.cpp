//
// Created by mayam on 04-Jun-21.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <pthread.h>
#include <vector>
use namespace std

// ------------------------------------------------------ declarations ---------------------------------------------
#define SYSTEM_ERROR "system error: "

typedef struct{
    const InputVec *inputVec;
    OutputVec *outputVec;
    std::vector<IntermediateVec> *intermediary_elements;
    std::vector<IntermediateVec> *output_elements;
}Context;

typedef struct{
    JobState state;
    pthread_t* threads = null;
    Context* contexts = null;
}Job;

void system_error(string message);
void sort();
void reduce();
void shuffle();
void wait_for_shuffle();
void basic_thread_entry();
void main_thread_entry();

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
    Job new_job;
    new_job.threads = new thread[multiThreadLevel];
    new_job.contexts = new Context[multiThreadLevel];
    for (int i = 0; i < multiThreadLevel; ++i) {
        new_job.threads[i] = pthread_create();
        new_job.contexts[i] = {inputVec, outputVec, new IntermediateVec, new OutputVec};
    }
    return nullptr;
}

void waitForJob(JobHandle job){
    return nullptr;
}

void getJobState(JobHandle job, JobState* state){
    Job* pointer = (Job*)job;
    *state = pointer->state;
}

void closeJobHandle(JobHandle job){
    return nullptr;
}


// ------------------------------------------------------ Auxiliary ---------------------------------------------


void system_error(string message){
    cout << SYSTEM_ERROR << message << "\n";
}

void basic_thread_entry(){
    map();
    sort();
    wait_for_shuffle();
    reduce();
}

void main_thread_entry(){
    map();
    sort();
    shuffle();
    reduce();
}