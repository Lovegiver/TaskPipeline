package com.citizenweb.tooling.taskpipeline.model;

import reactor.core.publisher.Flux;
import reactor.util.annotation.NonNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link Pipeline} contains all the logic needed to consume {@link Task}s in the most efficient way
 */
public class Pipeline {
    /** All the {@link Task} to process */
    private final Set<Task> tasks;

    public Pipeline(Set<Task> tasksToProcess) {
        this.tasks = tasksToProcess;
    }

    /** Converts a Collection into an Array */
    Function<Collection<Flux<?>>,Flux<?>[]> convertCollectionToArray = collection -> {
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
    public void execute() {
        Collection<Set<Task>> allPaths = computePaths(this.tasks);
        allPaths.parallelStream().forEach(path -> {
            WorkPath workPath = convertToWorkPath(path);
            CompletableFuture.supplyAsync( () -> this.processStartingTasks(workPath))
                    .thenApply(this::processIntermediateTasks)
                    .thenAccept(this::processFinalTasks)
                    .join();
        });
    }

    /**
     * What is a <b>path</b> ?<br>
     * A <b>path</b> is a collection of {@link Task}s, all of them are linked to each other by a previous/next relationship.<br>
     * Starting from a collection of tasks, we want to find out all the different paths.<br>
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
        var map = workPath.getTasksAndInputFluxesMap();
        while (! ( map.containsKey(workPath.getEndingTask()) && map.keySet().size() == 1 ) ) {
            Set<Task> tasksToRemoveFromMap = new HashSet<>();
            map.keySet()
                    .stream()
                    .filter(Task.isTerminalTask.negate())
                    .forEach(task -> {
                Flux<?> flux = task.process(this.convertCollectionToArray.apply(map.get(task)));
                task.getSuccessors().forEach(next -> workPath.injectFluxIntoNextTask.accept(next, flux));
                tasksToRemoveFromMap.add(task);
            });
            tasksToRemoveFromMap.forEach(map::remove);
            tasksToRemoveFromMap.clear();
        }
        return workPath;
    }

    /**
     * FinalTasks (or TerminalTasks) are the {@link Task}s we want to compute the resulting {@link Flux}.<br>
     */
    private void processFinalTasks(WorkPath workPath) {
        workPath.getTasksAndInputFluxesMap().forEach((key, value) -> {
            Flux<?> flux = key.process(this.convertCollectionToArray.apply(value));
            flux.log().subscribe(System.out::println);
        });
    }

    /**
     * Recursive function used to compute the whole path associated to a terminal {@link Task}.<br>
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
