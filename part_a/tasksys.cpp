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

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // Store the number of threads to use for parallel execution
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Create a shared atomic counter for dynamic task distribution
    std::atomic<int> next_task_id(0);
    
    // Vector to store all worker threads
    std::vector<std::thread> threads;
    
    // Lambda function that each thread will execute
    auto worker = [&]() {
        while (true) {
            // Atomically fetch the next task ID
            int task_id = next_task_id.fetch_add(1);
            
            // Check if all tasks are done
            if (task_id >= num_total_tasks) {
                break;
            }
            
            // Execute the task
            runnable->runTask(task_id, num_total_tasks);
        }
    };
    
    // Spawn worker threads
    for (int i = 0; i < num_threads_; i++) {
        threads.push_back(std::thread(worker));
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
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
    while (true) {
        // Check if we should shutdown
        if (shutdown_) {
            break;
        }
        
        // Spin until there's work (busy-wait)
        if (!has_work_) {
            continue;
        }
        
        // There's work available, try to grab a task
        while (true) {
            int task_id = next_task_id_.fetch_add(1);
            
            // Check if all tasks have been claimed
            if (task_id >= num_total_tasks_) {
                break;
            }
            
            // Execute the task
            current_runnable_->runTask(task_id, num_total_tasks_);
            
            // Mark task as completed
            tasks_completed_.fetch_add(1);
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Set up the task information for worker threads
    current_runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    next_task_id_ = 0;
    tasks_completed_ = 0;
    
    // Signal worker threads that there's work available
    has_work_ = true;
    
    // Spin-wait until all tasks are completed
    while (tasks_completed_ < num_total_tasks) {
        // Busy-wait (spinning)
    }
    
    // Reset the work flag for the next run() call
    has_work_ = false;
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
    // Set up the task information for worker threads
    current_runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    next_task_id_ = 0;
    tasks_completed_ = 0;
    
    // Signal worker threads that there's work available and wake them up
    {
        std::unique_lock<std::mutex> lock(mutex_);
        has_work_ = true;
    }
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
