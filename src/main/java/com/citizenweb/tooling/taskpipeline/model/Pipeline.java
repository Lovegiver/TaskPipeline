package com.citizenweb.tooling.taskpipeline.model;

import reactor.core.publisher.Flux;
import reactor.util.annotation.NonNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link Pipeline} contains all the logic needed to consume {@link Task}s in the most efficient way
 */
public class Pipeline {
    /** For a given {@link Task}, associated values are the input fluxes needed to execute the {@link Operation#process(Flux[])}. */
    private final Map<Task, Collection<Flux<?>>> tasksRelationships = new ConcurrentHashMap<>();
    /** All the {@link Task} to process */
    private final Set<Task> tasks;

    public Pipeline(Set<Task> tasksToProcess) {
        this.tasks = tasksToProcess;
    }

    /** Each time a {@link Flux} is produced, we have to inject it as an input of the next {@link Task}.<br>
     * The Task and its inputs are temporarily stored in a local {@link ConcurrentHashMap}.<br> */
    BiConsumer<Task,Flux<?>> injectFluxIntoNextTask = ((next, flux) -> {
        if (this.tasksRelationships.get(next) != null) {
            this.tasksRelationships.get(next).add(flux);
        } else {
            Set<Flux<?>> fluxSet = new HashSet<>();
            fluxSet.add(flux);
            this.tasksRelationships.put(next, fluxSet);
        }
    });

    /** Converts a Collection into an Array */
    Function<Collection<Flux<?>>,Flux<?>[]> convertCollectionToArray = collection -> {
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };

    /**
     * Do 4 steps :
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
            Set<Task> endingTasks = path.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
            Set<Task> startingTasks = path.stream().filter(Task.isInitialTask).collect(Collectors.toSet());
            this.processStartingTasks(startingTasks);
            this.processIntermediateTasks(endingTasks);
            this.processFinalTasks();
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
     * StartingTasks are all {@link Task}s without any predecessors (or previous Task).<br>
     * These {@link Task}s are specific because they do not need any input {@link Flux}.<br>
     * They all will be consumed by the following {@link Task}s.<br>
     * @param startingTasks all the {@link Task}s without any predecessors
     */
    private void processStartingTasks(Collection<Task> startingTasks) {
        startingTasks.forEach(task -> {
            Flux<?> flux = task.process(Flux.empty());
            task.getSuccessors().forEach(next -> this.injectFluxIntoNextTask.accept(next, flux));
        });
    }

    /**
     * IntermediateTasks have predecessors and successors.<br>
     * They all will be consumed, layer after layer, until we reach the terminal {@link Task}s.<br>
     * @param endingTasks we will consume all {@link Task}s but these
     */
    private void processIntermediateTasks(Collection<Task> endingTasks) {
        while (!endingTasks.containsAll(this.tasksRelationships.keySet())) {
            Set<Task> tasksToRemoveFromMap = new HashSet<>();
            this.tasksRelationships.keySet()
                    .stream()
                    .filter(Task.isTerminalTask.negate())
                    .forEach(task -> {
                Flux<?> flux = task.process(this.convertCollectionToArray.apply(this.tasksRelationships.get(task)));
                task.getSuccessors().forEach(next -> this.injectFluxIntoNextTask.accept(next, flux));
                tasksToRemoveFromMap.add(task);
            });
            tasksToRemoveFromMap.forEach(this.tasksRelationships::remove);
            tasksToRemoveFromMap.clear();
        }
    }

    /**
     * FinalTasks (or TerminalTasks) are the {@link Task}s we want to compute the resulting {@link Flux}.<br>
     */
    private void processFinalTasks() {
        this.tasksRelationships.forEach((key, value) -> {
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
