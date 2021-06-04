//
// Created by mayam on 04-Jun-21.
//
#include "MapReduceFramework.h"
#include <pthread.h>
use namespace std

#define SYSTEM_ERROR "system error: "


typedef job {
    stage_t state;
};


void emit2 (K2* key, V2* value, void* context){
    return;
}
void emit3 (K3* key, V3* value, void* context){
    return;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    return nullptr;
}

void waitForJob(JobHandle job){
    return nullptr;
}

void getJobState(JobHandle job, JobState* state){
    *state = job->state
}

void closeJobHandle(JobHandle job){
    return nullptr;
}


void system_error(string message){
    cout << SYSTEM_ERROR << message << "\n";
}
