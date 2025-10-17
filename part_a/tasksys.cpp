#include "tasksys.h"
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

 static void parallelSpawnWorker(std::atomic<int>* next_task_id, int num_total_tasks, IRunnable* runnable) {
    // dynamic assignment of tasks to worker threads
    while (true) {
        // Atomically fetch the next task Id
        int task_id = next_task_id->fetch_add(1);

        // check if all tasks already done
        if (task_id >= num_total_tasks) {
            break;
    }

    // execute the task if available
    runnable->runTask(task_id, num_total_tasks);
 }
}

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // Store the number of threads to use for parallel execution
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    // Create a shared atomic counter for dynamic task distribution; this is atomic so that when a thread is reading 
    // its task, no other thread can modify the counter / read the same task; since this is dynamic distribution of tasks. 
    std::atomic<int> next_task_id(0);
    
    // Vector to store all worker threads
    std::vector<std::thread> threads;

    // Spawn worker threads
    for (int i=0; i < num_threads_; i++) {
        threads.push_back(std::thread(parallelSpawnWorker, &next_task_id, num_total_tasks, runnable));
    }

    // wait for all threads to complete before returning
    for (int i = 0; i < num_threads_; i++) {
        threads[i].join();
    }
    
    
}


TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    shutdown_ = false;
    has_work_ = false;
    current_runnable_ = nullptr;
    next_task_id_ = 0;
    num_total_tasks_ = 0;
    tasks_completed_ = 0;
    
    // Create the thread pool
    for (int i = 0; i < num_threads_; i++) {
        threads_.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::workerThread, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // Signal all threads to shutdown
    shutdown_ = true;
    
    // Wait for all threads to finish
    for (auto& thread : threads_) {
        thread.join();
    }
}


void TaskSystemParallelThreadPoolSpinning::workerThread() {
    while (!shutdown_) {
        // Check if there's work available
        // Using load() with acquire ensures we see all state when has_work_ becomes true
        if (!has_work_.load(std::memory_order_acquire)) {
            continue;
        }
        
        // At this point, due to acquire-release synchronization, we're guaranteed
        // to see all state that was written before has_work_ was set to true
        // So we can read these variables directly without a lock!
        while (true) {
            int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
            
            // Check if all tasks have been claimed
            if (task_id >= num_total_tasks_) {
                break;
            }
            
            // Execute the task
            current_runnable_->runTask(task_id, num_total_tasks_);
            tasks_completed_.fetch_add(1, std::memory_order_relaxed);
        }
         // After all tasks claimed, wait for work flag to be reset
        // This reduces spinning after work is done
        // while (has_work_.load(std::memory_order_acquire)) {
        //     // spin
        // }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Lock mutex to set all state atomically
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        current_runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        next_task_id_ = 0;
        tasks_completed_ = 0;
    }
    
    // Signal workers that work is available
    // Using store() with release ensures workers see all above state
    has_work_.store(true, std::memory_order_release);
    
    // Spin-wait until all tasks are completed
    while (tasks_completed_.load(std::memory_order_relaxed) < num_total_tasks_) {
        // Can use relaxed here since we have proper synchronization elsewhere
    }
    
    // Signal workers to stop
    has_work_.store(false, std::memory_order_release);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    shutdown_ = false;
    has_work_ = false;
    current_runnable_ = nullptr;
    next_task_id_ = 0;
    num_total_tasks_ = 0;
    tasks_completed_ = 0;
    
    // Create the thread pool
    for (int i = 0; i < num_threads_; i++) {
        threads_.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerThread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Signal all threads to shutdown
    {
        std::unique_lock<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    
    // Wake up all sleeping threads so they can exit
    work_available_cv_.notify_all();
    
    // Wait for all threads to finish
    for (auto& thread : threads_) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        // Wait for work to become available (sleep until notified)
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_available_cv_.wait(lock, [this]() {
                return has_work_ || shutdown_;
            });
            
            // Check if we should shutdown
            if (shutdown_) {
                break;
            }
        }
        
        // There's work available, grab and execute tasks
        while (true) {
            int task_id = next_task_id_.fetch_add(1);
            
            // Check if all tasks have been claimed
            if (task_id >= num_total_tasks_) {
                break;
            }
            
            // Execute the task
            current_runnable_->runTask(task_id, num_total_tasks_);
            
            // Mark task as completed
            int completed = tasks_completed_.fetch_add(1) + 1;
            
            // If this was the last task, notify the waiting thread
            if (completed == num_total_tasks_) {
                std::unique_lock<std::mutex> lock(mutex_);
                work_done_cv_.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Set up the task information for worker threads and signal them
    // All state must be set atomically while holding the lock
    {
        std::unique_lock<std::mutex> lock(mutex_);
        current_runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        next_task_id_ = 0;
        tasks_completed_ = 0;
        has_work_ = true;
    }
    // Notify must be done after releasing the lock for better performance
    work_available_cv_.notify_all();
    
    // Wait until all tasks are completed
    {
        std::unique_lock<std::mutex> lock(mutex_);
        work_done_cv_.wait(lock, [this]() {
            return tasks_completed_ >= num_total_tasks_;
        });
        
        // Reset the work flag for the next run() call
        has_work_ = false;
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}