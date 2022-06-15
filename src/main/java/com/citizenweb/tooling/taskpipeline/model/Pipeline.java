package com.citizenweb.tooling.taskpipeline.model;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.util.annotation.NonNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A {@link Pipeline} contains all the logic needed to consume {@link Task}s in the most efficient way
 */
@Slf4j
public class Pipeline {
    /**
     * All the {@link Task} to process
     */
    @Getter
    private final Set<Task> tasks;


    private final PathOptimizer optimizer;

    private final ConcurrentHashMap<String, CompletableFuture<?>> runningWorkPaths = new ConcurrentHashMap<>();

    public Pipeline(Set<Task> tasksToProcess) {
        this.tasks = tasksToProcess;
        this.optimizer = PathOptimizer.DEFAULT_OPTIMIZER;
    }

    public Pipeline(Set<Task> tasksToProcess, PathOptimizer optimizer) {
        this.tasks = tasksToProcess;
        this.optimizer = optimizer;
    }

    /**
     * From the given tasks, compute all possible paths, ie all the tasks to process
     * in order to complete a 'terminal' (final, ending) {@link Task}
     */
    public Map<String, CompletableFuture<?>> execute() {
        Collection<WorkPath> workPaths = this.optimizer.optimize(this.tasks);
        log.info("Found {} work paths", workPaths.size());
        workPaths.parallelStream().forEach(workPath -> {
            CompletableFuture<?> future = workPath.execute();
            runningWorkPaths.put(workPath.getName(), future);
        });
        return this.runningWorkPaths;
    }

}
