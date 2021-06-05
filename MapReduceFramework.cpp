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
#include <algorithm>
using namespace std;

// ------------------------------------------------------ declarations ---------------------------------------------
#define SYSTEM_ERROR "system error: "
#define THREAD_CREATE_ERROR "system couldn't create thread: "
#define FAIL 1
#define SUCCESS 0
#define INTER_BACK job->contexts[i].my_intermediary->back()
#define INTER_EMPTY job->contexts[i].my_intermediary->empty()
#define THREAD0_INTER_BACK job->contexts[0].my_intermediary->back().first

struct Context{
    // todo: hold job?
    const InputVec *inputVec;
    OutputVec *outputVec;
    IntermediateVec* my_intermediary;
    vector<IntermediateVec*>* intermediary_elements;
    JobState* job_state;
    atomic<int>* map_counter;
    atomic<int>* intermediary_counter;
    atomic<int>* out_counter;
    const MapReduceClient* client;
};

typedef struct{
    JobState* state;
    pthread_t* threads = NULL;
    Context* contexts = NULL;
    int multiThreadLevel;
}Job;

//Job new_job(InputVec *inputVec, OutputVec *outputVec, pthread_t* threads, Context* contexts, int size){
//    Job
//}

void handle_error(int code, string message);
void map(Context* tc);
void sort_(Context* tc);
void reduce(Context* tc);
void shuffle(Context* tc);
void wait_for_shuffle(Context* tc);
void* basic_thread_entry(void *);
void* main_thread_entry(void *);
void* f(void *);
// ------------------------------------------------------ API ---------------------------------------------

void emit2 (K2* key, V2* value, void* context){
    Context* tc = (Context*) context;
    if (tc == nullptr){
        return; // todo: what to do?
    }
    tc->my_intermediary->push_back({key, value});

    (*(tc->intermediary_counter))++;
}

void emit3 (K3* key, V3* value, void* context){
    Context* tc = (Context*) context;
    if (tc == nullptr){
        return; // todo: what to do?
    }
    tc->outputVec->push_back({key, value});
    (*(tc->intermediary_counter))++;
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
    atomic<int>* map_counter(0);
    atomic<int>* inter_counter(0);
    atomic<int>* out_counter(0);

    // create job
    Job* new_job;
    new_job->state->stage = UNDEFINED_STAGE;
    new_job->threads = new pthread_t[multiThreadLevel];
    new_job->contexts = new Context[multiThreadLevel];
    new_job->multiThreadLevel = multiThreadLevel;

    //thread 0
    int err = pthread_create(&new_job->threads[0], NULL, &main_thread_entry, NULL);
    handle_error(err, THREAD_CREATE_ERROR);
    new_job->contexts[0].intermediary_elements = new vector<IntermediateVec*>();
    new_job->contexts[0].outputVec = new OutputVec;

    // rest of threads
    for (int i = 1; i < multiThreadLevel; ++i) {
        int err = pthread_create(&new_job->threads[i], NULL, &basic_thread_entry, NULL);
        handle_error(err, THREAD_CREATE_ERROR);
        new_job->contexts[i].job_state = new_job->state;
        new_job->contexts[i].my_intermediary =  new IntermediateVec;
        new_job->contexts[i].outputVec = new OutputVec;
        new_job->contexts[i].map_counter = map_counter;
        new_job->contexts[i].intermediary_counter = inter_counter;
        new_job->contexts[i].out_counter = out_counter;
        new_job->contexts[i].inputVec = new_inputVec;
        new_job->contexts[i].outputVec = new_outVec;
        new_job->contexts[i].client = &client;
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

void* basic_thread_entry(void * arg){
    Context* tc = (Context*) arg;
    if (tc == nullptr){
        return nullptr; // todo: what to do?
    }
    map(tc);
    sort_(tc);
    wait_for_shuffle(tc);
    reduce(tc);
    return nullptr;
}


void* main_thread_entry(void * arg){
    Context* tc = (Context*) arg;
    if (tc == nullptr){
        return nullptr; // todo: what to do?
    }
    map(tc);
    sort_(tc);
    shuffle(tc);
    reduce(tc);
    return nullptr;
}


void* f(void *){
    printf("!\n");
    return nullptr;
}

void map(Context* tc){
    printf("1\n");
    tc->intermediary_elements->push_back(tc->my_intermediary);
    auto iter = tc->inputVec->begin();
    int i=0;
    while (*iter != *tc->inputVec->end()){
        if (i == *tc->map_counter){
            tc->client->map(iter->first, iter->second, tc);
            (*(tc->map_counter))++;
            iter++;
        }
        i++;
        iter++;
    }
    return;
}

void sort_(Context* tc){
    sort(tc->intermediary_elements->begin(), tc->intermediary_elements->end());
    printf("2\n");
    return;
}

void shuffle(Job* job, Context thread0) {
    printf("3\n");
    vector<IntermediateVec*>* queue;
    int count = job->contexts[0].intermediary_elements->size();
    K2* key = THREAD0_INTER_BACK;
    queue->push_back(new IntermediateVec());
    while (count > 0){
        for (int i = 0; i < job->multiThreadLevel; ++i) {
            while(!INTER_EMPTY && *key < *(INTER_BACK.first) && *(INTER_BACK.first) < *key) {
                queue->back()->push_back(INTER_BACK);
                count--;
            }
        }
        key = THREAD0_INTER_BACK;
        queue->push_back(new IntermediateVec());
    }
    job->contexts[0].intermediary_elements = queue;
    return;
}

void reduce(Context* tc){
    printf("5\n");
    printf("1\n");
    auto iter = tc->intermediary_elements->begin();
    int i=0;
    while (*iter != *tc->intermediary_elements->end()){
        if (i == *tc->map_counter){
            tc->client->reduce(*iter, tc);
            (*(tc->map_counter))++;
            iter++;
        }
        i++;
        iter++;
    }
    return;
}

void wait_for_shuffle(Context* tc){
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