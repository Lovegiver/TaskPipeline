package com.citizenweb.tooling.taskpipeline.model;

import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.util.annotation.NonNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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
     * Converts a Collection into an Array
     */
    Function<Collection<Flux<?>>, Flux<?>[]> convertCollectionToArray = collection -> {
        Objects.requireNonNull(collection, "Collection is NULL thus can't be converted into Array");
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };

    /**
     * Does 4 steps :
     * <ol>
     *     <li>from the given tasks, compute all possible paths, ie all the tasks to process in order to complete a terminal task</li>
     *     <li>process all starting tasks</li>
     *     <li>process all intermediate tasks</li>
     *     <li>process all terminal tasks</li>
     * </ol>
     */
    public Map<String, CompletableFuture<?>> execute() {
        Collection<Set<Task>> allPaths = computePaths(this.tasks);
        allPaths.forEach(path -> {
            String name = path.stream().filter(Task.isTerminalTask).map(Task::getTaskName).findAny().orElseThrow();
            var x = CompletableFuture.supplyAsync(() -> convertToWorkPath(path))
                    .thenApply(this::processStartingTasks)
                    .thenApply(this::processIntermediateTasks)
                    .thenApply(this::processFinalTasks)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Error occurred : " + ex.getCause());
                        } else {
                            log.info("Finished processing task : " + name);
                        }
                    });
            runningWorkPaths.put(name, x);
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
    private Collection<Set<Task>> computePaths(@NonNull Set<Task> tasks) {
        List<Set<Task>> allPaths = new ArrayList<>();
        Set<Task> terminalTasks = tasks.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        for (Task task : terminalTasks) {
            Set<Task> pathForTask = new HashSet<>();
            this.findAllTasksFromTree(pathForTask, task);
            allPaths.add(pathForTask);
        }
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
     * StartingTasks are all {@link Task}s without any predecessors (or previous Task).<br>
     * These {@link Task}s are specific because they do not need any input {@link Flux}.<br>
     * They all will be consumed by the following {@link Task}s.<br>
     *
     * @param workPath contains all the {@link Task}s without any predecessors
     * @return a {@link WorkPath}
     */
    private WorkPath processStartingTasks(WorkPath workPath) {
        workPath.getStartingTasks().forEach(task -> {
            Flux<?> flux = task.process(Flux.empty());
            task.getSuccessors().forEach(next -> workPath.injectFluxIntoNextTask.accept(next, flux));
        });
        return workPath;
    }

    /**
     * IntermediateTasks have predecessors and successors.<br>
     * They all will be consumed, layer after layer, until we reach the terminal {@link Task}s.<br>
     *
     * @param workPath contains all {@link Task}s to be consumed but ending tasks
     */
    private WorkPath processIntermediateTasks(WorkPath workPath) {
        Map<Task,Collection<Flux<?>>> map = workPath.getTasksAndInputFluxesMap();
        while (!(workPath.getTasksAndInputFluxesMap().containsKey(workPath.getEndingTask())
                && workPath.getTasksAndInputFluxesMap().keySet().size() == 1)) {
            Set<Task> tasksToRemoveFromMap = new HashSet<>();
            workPath.getTasksAndInputFluxesMap().keySet()
                    .stream()
                    .filter(Task.isTerminalTask.negate())
                    .forEach(task -> {
                        log.warn("Processing task {}", task.getTaskName());
                        Flux<?> flux = task.process(this.convertCollectionToArray.apply(workPath.getTasksAndInputFluxesMap().get(task)));
                        task.getSuccessors().forEach(next -> workPath.injectFluxIntoNextTask.accept(next, flux));
                        tasksToRemoveFromMap.add(task);
                    });
            workPath.cleanMap.accept(tasksToRemoveFromMap);
        }
        return workPath;
    }

    /**
     * FinalTasks (or TerminalTasks) are the {@link Task}s we want to compute the resulting {@link Flux}.<br>
     */
    private WorkPath processFinalTasks(WorkPath workPath) {
        workPath.getTasksAndInputFluxesMap().forEach((task, inputFluxes) -> {
            Flux<?> flux = task.process(this.convertCollectionToArray.apply(inputFluxes));
            flux.log().subscribe(log::info);
        });
        return workPath;
    }

    /**
     * Recursive function used to compute the whole path associated to a terminal {@link Task}.<br>
     *
     * @param path the path to build
     * @param task the reference {@link Task}
     */
    void findAllTasksFromTree(Set<Task> path, Task task) {
        path.add(task);
        if (!Task.isInitialTask.test(task)) {
            for (Task t : task.getPredecessors()) {
                this.findAllTasksFromTree(path, t);
            }
        }
    }

}
