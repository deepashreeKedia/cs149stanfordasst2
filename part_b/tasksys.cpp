#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    next_launch_id_ = 0;
    total_pending_tasks_ = 0;
    
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
    
    // Clean up allocated bulk task launches
    for (auto& pair : all_launches_) {
        delete pair.second;
    }
}

void TaskSystemParallelThreadPoolSleeping::checkAndEnqueueLaunch(TaskID launch_id) {
    // Must be called with mutex_ held!
    BulkTaskLaunch* launch = all_launches_[launch_id];
    
    if (launch->dependencies.empty() && !launch->is_ready) {
        launch->is_ready = true;
        
        // Enqueue all tasks from this bulk launch into the ready queue
        for (int i = 0; i < launch->num_total_tasks; i++) {
            Task task;
            task.launch = launch;  // Store pointer instead of ID
            task.runnable = launch->runnable;
            task.task_id = i;
            task.num_total_tasks = launch->num_total_tasks;
            ready_queue_.push(task);
        }
        
        // Wake up worker threads
        work_available_cv_.notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        Task task;
        bool has_task = false;
        
        // Wait for work or shutdown signal
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_available_cv_.wait(lock, [this]() {
                return !ready_queue_.empty() || shutdown_;
            });
            
            if (shutdown_) {
                break;
            }
            
            // Grab a task from the ready queue
            if (!ready_queue_.empty()) {
                task = ready_queue_.front();
                ready_queue_.pop();
                has_task = true;
            }
        }
        
        // Execute the task outside the lock
        if (has_task) {
            task.runnable->runTask(task.task_id, task.num_total_tasks);
            
            // Mark this task as completed
            BulkTaskLaunch* launch = task.launch;
            int completed = launch->tasks_completed.fetch_add(1) + 1;
            
            // If this bulk launch is complete, update dependencies
            if (completed == launch->num_total_tasks) {
                std::unique_lock<std::mutex> lock(mutex_);
                
                // Notify all launches that depend on this one
                for (TaskID dependent_id : launch->dependent_launches) {
                    BulkTaskLaunch* dependent = all_launches_[dependent_id];
                    dependent->dependencies.erase(launch->launch_id);
                    
                    // If dependent now has no dependencies, it's ready
                    if (dependent->dependencies.empty() && !dependent->is_ready) {
                        checkAndEnqueueLaunch(dependent_id);
                    }
                }
                
                // Decrement total pending tasks
                int pending = total_pending_tasks_.fetch_sub(launch->num_total_tasks) - launch->num_total_tasks;
                
                // If all tasks are done, notify sync()
                if (pending == 0) {
                    all_done_cv_.notify_one();
                }
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Implement run() using runAsyncWithDeps() and sync()
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::unique_lock<std::mutex> lock(mutex_);

    // Generate a unique TaskID for this bulk launch
    TaskID launch_id = next_launch_id_.fetch_add(1);
    
    // Create a new BulkTaskLaunch
    BulkTaskLaunch* launch = new BulkTaskLaunch();
    launch->launch_id = launch_id;
    launch->runnable = runnable;
    launch->num_total_tasks = num_total_tasks;
    launch->tasks_completed = 0;
    launch->is_ready = false;
    
    // Track dependencies
    for (TaskID dep_id : deps) {
        launch->dependencies.insert(dep_id);
        // Register this launch as a dependent of its dependencies
        all_launches_[dep_id]->dependent_launches.insert(launch_id);
    }
    
    // Store this launch
    all_launches_[launch_id] = launch;
    
    // Increment total pending tasks
    total_pending_tasks_.fetch_add(num_total_tasks);
    
    // Check if this launch is ready to execute (no dependencies)
    checkAndEnqueueLaunch(launch_id);
    
    return launch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // Wait until all pending tasks are completed
    all_done_cv_.wait(lock, [this]() {
        return total_pending_tasks_ == 0;
    });
}
