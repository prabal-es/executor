package com.opentext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

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
        this.concurrencySemaphore = new Semaphore(maxConcurrency, true);
        //new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        // Create a task wrapper that ensures ordering and group locking
        Supplier<T> wrappedTask = () -> {
            try {
                // Acquire the concurrency limit
                concurrencySemaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // Lock the task group to ensure no concurrent execution of tasks within the same group
            ReentrantLock groupLock = groupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new ReentrantLock(true));
            groupLock.lock();
            try {
                // Execute the actual task
                return task.taskAction().call();
            } catch (Exception e) {
                throw new RuntimeException(e);
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
        // Return a CompletableFuture that is submitted to the executor service
        return CompletableFuture.supplyAsync(wrappedTask, executorService);
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        TaskExecutorService taskExecutorService = new TaskExecutorService(5);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        List<Future<Integer>> taskFutures = new ArrayList<>();
        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group1, TaskType.WRITE, () -> {
                System.out.println("Executing Task 1");
                TimeUnit.SECONDS.sleep(4);
                return 1;
            }))
        );

        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group1, TaskType.READ, () -> {
                System.out.println("Executing Task 2");
                TimeUnit.SECONDS.sleep(3);
                return 2;
            }))
        );

        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group2, TaskType.WRITE, () -> {
                System.out.println("Executing Task 3");
                TimeUnit.SECONDS.sleep(2);
                return 3;
            }))
        );

        taskFutures.add(taskExecutorService.submitTask(new Task<>(
                UUID.randomUUID(), group2, TaskType.WRITE, () -> {
                System.out.println("Executing Task 4");
                TimeUnit.SECONDS.sleep(1);
                return 4;
            }))
        );

        for(Future<Integer> task : taskFutures){
            if(task instanceof CompletableFuture){
                ((CompletableFuture<Integer>)task).thenAccept(taskExecutorService::printResult);
            }
        }

        taskExecutorService.shutdown();
    }

    private void printResult(int result){
        System.out.println("Task result: " + result);
    }
}
