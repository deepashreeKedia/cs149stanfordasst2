#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <set>
#include <map>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        // Forward declaration
        struct BulkTaskLaunch;
        
        // Structure to represent a single task within a bulk launch
        struct Task {
            BulkTaskLaunch* launch;  // Pointer to the bulk launch this task belongs to
            IRunnable* runnable;     // The runnable to execute
            int task_id;             // Task index within the bulk launch
            int num_total_tasks;     // Total tasks in this bulk launch
        };
        
        // Structure to track a bulk task launch
        struct BulkTaskLaunch {
            TaskID launch_id;
            IRunnable* runnable;
            int num_total_tasks;
            std::set<TaskID> dependencies;        // TaskIDs this launch depends on
            std::atomic<int> tasks_completed;     // How many tasks have completed
            std::set<TaskID> dependent_launches;  // TaskIDs that depend on this launch
            bool is_ready;                        // Whether all dependencies are satisfied
        };
        
        int num_threads_;
        std::vector<std::thread> threads_;
        
        // Task launch tracking
        std::atomic<TaskID> next_launch_id_;
        std::map<TaskID, BulkTaskLaunch*> all_launches_;  // All bulk task launches
        
        // Ready queue: tasks that can be executed
        std::queue<Task> ready_queue_;
        
        // Control flags
        std::atomic<bool> shutdown_;
        std::atomic<int> total_pending_tasks_;  // Total tasks not yet completed
        
        // Synchronization primitives
        std::mutex mutex_;
        std::condition_variable work_available_cv_;  // Signals workers when work is ready
        std::condition_variable all_done_cv_;        // Signals sync() when all work is done
        
        // Worker thread function
        void workerThread();
        
        // Helper function to check if a launch's dependencies are satisfied
        void checkAndEnqueueLaunch(TaskID launch_id);
};

#endif
