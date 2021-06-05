//
// Created by mayam on 04-Jun-21.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "Sample_Client\SampleClient.cpp"
#include <pthread.h>
#include <vector>
#include <string>
#include <iostream>
#include <atomic>
using namespace std;

// ------------------------------------------------------ declarations ---------------------------------------------
#define SYSTEM_ERROR "system error: "
#define THREAD_CREATE_ERROR "system couldn't create thread: "
#define FAIL 1
#define SUCCESS 0

struct Context{
    // todo: hold job?
    const InputVec *inputVec;
    OutputVec *outputVec;
    JobState* job_state;
    IntermediateVec* intermediary_elements;
    OutputVec* output_elements;
    atomic<int>* atomic_counter;
};

typedef struct{
    MapReduceClient* client;
    JobState* state;
    pthread_t* threads = NULL;
    Context* contexts = NULL;
}Job;

//Job new_job(InputVec *inputVec, OutputVec *outputVec, pthread_t* threads, Context* contexts, int size){
//    Job
//}

void handle_error(int code, string message);
void * map(void * args);
void sort();
void reduce();
void shuffle();
void wait_for_shuffle();
void* basic_thread_entry(void *);
void* main_thread_entry(void *);
void* f(void *);
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

    // valid:
    if (&inputVec == nullptr){
        return SUCCESS; // todo: or FAIL?
    }


    // create pointers:
    const InputVec* new_inputVec = &inputVec;
    OutputVec * new_outVec = &outputVec;
    atomic<int>* atomic_counter(0);

    // create job
    Job* new_job;
    new_job->state->stage = UNDEFINED_STAGE;
    new_job->threads = new pthread_t[multiThreadLevel];
    new_job->contexts = new Context[multiThreadLevel];

    //thread 0
    int err = pthread_create(&new_job->threads[0], NULL, &main_thread_entry, NULL);
    handle_error(err, THREAD_CREATE_ERROR);
    new_job->contexts[0].intermediary_elements =  new IntermediateVec;
    new_job->contexts[0].output_elements = new OutputVec;

    // rest of threads
    for (int i = 1; i < multiThreadLevel; ++i) {
        int err = pthread_create(&new_job->threads[i], NULL, &basic_thread_entry, NULL);
        handle_error(err, THREAD_CREATE_ERROR);
        new_job->contexts[i].job_state = new_job->state;
        new_job->contexts[i].intermediary_elements =  new IntermediateVec;
        new_job->contexts[i].output_elements = new OutputVec;
        new_job->contexts[i].atomic_counter = atomic_counter;
        new_job->contexts[i].inputVec = new_inputVec;
        new_job->contexts[i].outputVec = new_outVec;
    }
    return new_job;
}

void waitForJob(JobHandle job){
    return;
}

void getJobState(JobHandle job, JobState* state){
    Job* pointer = (Job*)job;
    *state = *pointer->state;
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
    map(nullptr);
    sort();
    wait_for_shuffle();
    reduce();
    return nullptr;
}


void* main_thread_entry(void *){
    map(nullptr);
    sort();
    shuffle();
    reduce();
    return nullptr;
}


void* f(void *){
    printf("!\n");
    return nullptr;
}

void* map(void* arg){
    Context* tc = (Context*) arg;
    if (tc == nullptr){
        return 0;
    }
    printf("1\n");
    for (auto i : *tc->inputVec) {
        int old_value = (*(tc->atomic_counter))++;
        (void) old_value;  // ignore not used warning
    }
    return 0;
}

void sort(){
    printf("2\n");
    return;
}

void shuffle() {
    printf("3\n");
    return;
}

void reduce(){
    printf("5\n");
    return;
}

void wait_for_shuffle(){
    printf("4\n");
    return;
}


int main(int argc, char** argv){
    //CounterClient client = new CounterClient()
    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;
    JobHandle out = startMapReduceJob(client, inputVec, outputVec, 5);
    Job* a = (Job*)(out);
    return 0;
}