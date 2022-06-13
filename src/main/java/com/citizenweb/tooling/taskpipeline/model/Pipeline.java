package com.citizenweb.tooling.taskpipeline.model;

import lombok.extern.log4j.Log4j2;
import reactor.util.annotation.NonNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A {@link Pipeline} contains all the logic needed to consume {@link Task}s in the most efficient way
 */
@Log4j2
public class Pipeline {
    /**
     * All the {@link Task} to process
     */
    private final Set<Task> tasks;

    private final ConcurrentHashMap<String, CompletableFuture<?>> runningWorkPaths = new ConcurrentHashMap<>();

    public Pipeline(Set<Task> tasksToProcess) {
        this.tasks = tasksToProcess;
    }

    /**
     * From the given tasks, compute all possible paths, ie all the tasks to process
     * in order to complete a 'terminal' (final, ending) {@link Task}
     */
    public Map<String, CompletableFuture<?>> execute() {
        Collection<Set<Task>> allPaths = computeAllDistinctPaths(this.tasks);
        allPaths.parallelStream().forEach(path -> {
            WorkPath workPath = convertToWorkPath(path);
            CompletableFuture<?> future = workPath.execute();
            runningWorkPaths.put(workPath.getName(), future);
        });
        return this.runningWorkPaths;
    }

    /**
     * What is a <b>path</b> ?<br>
     * A <b>path</b> is a collection of {@link Task}s, all of them are linked to each other by a previous/next relationship.<br>
     * Starting from a collection of tasks, we want to find out all the different paths.<br>
     *
     * @param tasks the whole collection of tasks
     * @return a Collection of Sets of {@link Task}s with each Set a specific path
     */
    @NonNull
    private Collection<Set<Task>> computeAllDistinctPaths(@NonNull Set<Task> tasks) {
        List<Set<Task>> allPaths = new ArrayList<>();
        Set<Task> terminalTasks = tasks.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        for (Task task : terminalTasks) {
            Set<Task> pathForTask = new HashSet<>();
            this.findAllTasksFromTree(pathForTask, task);
            allPaths.add(pathForTask);
        }
        log.info("{} 'work path' found", allPaths.size());
        return allPaths;
    }

    /**
     * Converts a Set of Tasks into a {@link WorkPath} object
     *
     * @param path the path to be converted
     * @return a {@link WorkPath} wrapping a Set of tasks
     */
    @NonNull
    private WorkPath convertToWorkPath(Set<Task> path) {
        return new WorkPath(path);
    }

    /**
     * Recursive function used to compute the whole path associated to a terminal {@link Task}.<br>
     *
     * @param path the path to build
     * @param task the reference {@link Task}
     */
    private void findAllTasksFromTree(Set<Task> path, Task task) {
        path.add(task);
        if (!Task.isInitialTask.test(task)) {
            for (Task t : task.getPredecessors()) {
                this.findAllTasksFromTree(path, t);
            }
        }
    }

}
