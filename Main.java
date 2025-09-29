
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.RejectedExecutionException;


class TaskExecutorImpl implements Main.TaskExecutor {
    private final ExecutorService executor;
    private final BlockingQueue<FutureTask<?>> taskQueue;
    private final ConcurrentHashMap<Main.TaskGroup, ReentrantLock> groupLocks = new ConcurrentHashMap<>();
    private Thread singleDispatcherThread;
    private volatile boolean isRunning = true;


    public TaskExecutorImpl(int poolSize, int queueSize) {
        this.executor = Executors.newFixedThreadPool(poolSize);
        this.taskQueue = new LinkedBlockingQueue<>(queueSize);
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        if(task == null) {
            throw new IllegalArgumentException("Task must not be null");
        }

        FutureTask<T> futureTask = new FutureTask<>(() -> {
            ReentrantLock lock = groupLocks.computeIfAbsent(task.taskGroup(), g -> new ReentrantLock());
            lock.lock();
            try {
                return task.taskAction().call();
            } catch (Exception e) {
                System.err.println("Task Failed. TaskID=" + task.taskUUID() + " --- " + e.getMessage());
                throw new ExecutionException("Task Execution failed for the task " + task.taskUUID(), e);
            } finally {
                lock.unlock();
            }
        });


        if(!taskQueue.offer(futureTask)) {
            String msg = "Task queue is full, unable to new accept task: " + task.taskUUID();
            System.err.println(msg);
            throw new RejectedExecutionException(msg);
        }
        return futureTask;
    }

    public void startExecution() {
        Runnable dispatchWork = () -> {
            while (isRunning) {
                try {
                    FutureTask<?> task = taskQueue.take();
                    if (task != null) {
                        executor.submit(task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Dispatcher Thread interrupted. Stopping gracefully...");
                    break;
                } catch (RejectedExecutionException e) {
                    System.err.println("Task rejected by executor: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("Unexpected error in dispatcher: " + e.getMessage());
                }
            }
        };
        singleDispatcherThread = new Thread(dispatchWork, "Task-Dispatcher");
        singleDispatcherThread.setDaemon(true);
        singleDispatcherThread.start();
        System.out.println("Dispatcher thread started");
    }

    public void shutdown() {
        isRunning = false;
        if (singleDispatcherThread != null) {
            singleDispatcherThread.interrupt();
            try {
                singleDispatcherThread.join(2000);
                System.out.println("Dispatcher thread terminated.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted while waiting for dispatcher to stop.");
            }
        }

        executor.shutdown();
        System.out.println("Shutdown initiated. Waiting for tasks to complete...");
        try {
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                System.err.println("Forcing shutdown: Not all tasks terminated in time");
                executor.shutdownNow();
            } else {
                System.out.println("All tasks completed, executor shutdown cleanly.");
            }
        } catch (InterruptedException e) {
            System.err.println("Shutdown interrupted, forcing shutdown");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Executor service shutdown complete");
    }
}

public class Main {

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID   Unique task identifier.
     * @param taskGroup  Task group.
     * @param taskType   Task type.
     * @param taskAction Callable representing task computation and returning the
     *                   result.
     * @param <T>        Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }
    public static void main(String[] args) {
        TaskExecutorImpl executor = new TaskExecutorImpl(10, 100);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        Task t1 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
            Thread.sleep(1000);
            return "Task 1 completed";
        });

        Task t2 = new Task<>(UUID.randomUUID(), group1, TaskType.WRITE, () -> {
            Thread.sleep(2000);
            return "Task 2 completed";
        });

        Task t3 = new Task<>(UUID.randomUUID(), group2, TaskType.WRITE, () -> {
            Thread.sleep(5000);
            return "Task 3 completed";
        });
        Task t4 = new Task<>(UUID.randomUUID(), group2, TaskType.READ, () -> {
            Thread.sleep(1000);
            return "Task 4 completed";
        });


        List<Future<String>> futures = new ArrayList<>();

        List<Task<String>> tasks = List.of(t1, t2, t3, t4);
        for (Task<String> task : tasks) {
            futures.add(executor.submitTask(task));
        }
        executor.startExecution();

        try {
            for (Future<String> future : futures) {
                System.out.println(future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        executor.shutdown();
    }
}
