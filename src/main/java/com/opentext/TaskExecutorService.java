package com.opentext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.opentext.Main.TaskExecutor;
import com.opentext.Main.Task;
import com.opentext.Main.TaskType;
import com.opentext.Main.TaskGroup;

public class TaskExecutorService implements TaskExecutor {

    private final ExecutorService executorService;
    private final Semaphore concurrencySemaphore;
    private final ConcurrentHashMap<UUID, ReentrantLock> groupLocks = new ConcurrentHashMap<>();


    public TaskExecutorService(int maxConcurrency) {
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
        //new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        this.concurrencySemaphore = new Semaphore(maxConcurrency);
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        // Create a task wrapper that ensures ordering and group locking
        Callable<T> wrappedTask = () -> {
            // Acquire the concurrency limit
            concurrencySemaphore.acquire();

            // Lock the task group to ensure no concurrent execution of tasks within the same group
            ReentrantLock groupLock = groupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new ReentrantLock());
            groupLock.lock();

            try {
                // Execute the actual task
                return task.taskAction().call();
            } finally {
                // Release the group lock and concurrency semaphore
                groupLock.unlock();
                concurrencySemaphore.release();

                // Clean up the group lock if no other tasks are waiting
                if (!groupLock.hasQueuedThreads()) {
                    groupLocks.remove(task.taskGroup().groupUUID(), groupLock);
                }
            }
        };

        // Return a Future that is submitted to the executor service
        return executorService.submit(wrappedTask);
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(18, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutorService = new TaskExecutorService(3);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        List<Future<Integer>> taskFutures = new ArrayList<>();
        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group1, TaskType.WRITE, () -> {
                System.out.println("Executing Task 1");
                Thread.sleep(3000);
                return 1;
            }))
        );

        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group1, TaskType.READ, () -> {
                System.out.println("Executing Task 2");
                Thread.sleep(2000);
                return 2;
            }))
        );

        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group2, TaskType.WRITE, () -> {
                System.out.println("Executing Task 3");
                Thread.sleep(3000);
                return 3;
            }))
        );

        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group2, TaskType.WRITE, () -> {
                System.out.println("Executing Task 4");
                Thread.sleep(2000);
                return 4;
            }))
        );

        for(int i = 0; i < taskFutures.size(); i++){
            System.out.println("Task "+(i+1)+" result: " + taskFutures.get(i).get());
        }

        taskExecutorService.shutdown();
    }
}
