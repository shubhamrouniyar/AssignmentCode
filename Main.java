
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


class TaskExecutorImpl implements Main.TaskExecutor {
    private final ExecutorService executor;
    private final BlockingQueue<FutureTask<?>> taskQueue;
    private final ConcurrentHashMap<Main.TaskGroup, ReentrantLock> groupLocks = new ConcurrentHashMap<>();
    private Thread singleDispatcherThread;


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
            } finally {
                lock.unlock();
            }
        });


        if(!taskQueue.offer(futureTask)) {
            throw new RuntimeException("Task queue is full");
        }
        return futureTask;
    }

    public void startExecution() {
        Runnable dispatchWork = () -> {
            while (true) {
                try {
                    FutureTask<?> task = taskQueue.take();
                    if (task != null) {
                        executor.submit(task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };
        singleDispatcherThread = new Thread(dispatchWork);
        singleDispatcherThread.start();
    }

    public void shutdown() {
        if (singleDispatcherThread != null) {
            singleDispatcherThread.interrupt();
        }
        executor.shutdown();
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
        void shutdown();
        void startExecution();
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
        TaskExecutor executor = new TaskExecutorImpl(10, 100);

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
