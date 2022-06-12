package com.citizenweb.tooling.taskpipeline.model;

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
    /**
     * Converts a Collection into an Array
     */
    Function<Collection<Flux<?>>, Flux<?>[]> convertCollectionToArray = collection -> {
        Objects.requireNonNull(collection, "Collection is NULL thus can't be converted into Array");
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };

    Function<Collection<Optional<Flux<?>>>, Collection<Flux<?>>> removeOptional = optionals -> {
        Objects.requireNonNull(optionals, "Collection is NULL thus Optional can't be removed");
        Collection<Flux<?>> fluxes = new ArrayList<>(optionals.size());
        optionals.forEach(optional -> fluxes.add(optional.orElseThrow()));
        return fluxes;
    };

    public Pipeline(Set<Task> tasksToProcess) {
        this.tasks = tasksToProcess;
    }

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
        Collection<Set<Task>> allPaths = computeAllDistinctPaths(this.tasks);
        allPaths.parallelStream().forEach(path -> {
            String name = path.stream().filter(Task.isTerminalTask).map(Task::getName).findAny().orElseThrow();
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
    private Collection<Set<Task>> computeAllDistinctPaths(@NonNull Set<Task> tasks) {
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
        log.info("Processing {} 'starting' tasks", workPath.getStartingTasks().size());
        workPath.getStartingTasks().forEach(currentTask -> {
            Flux<?> flux = currentTask.process(Flux.empty());
            currentTask.getSuccessors()
                    .stream()
                    .filter(workPath::taskBelongsToWorkPath)
                    .forEach(nextTask -> workPath.injectFlux(currentTask, nextTask, flux));
        });
        log.info("Done");
        return workPath;
    }

    /**
     * IntermediateTasks have predecessors and successors.<br>
     * They all will be consumed, layer after layer, until we reach the terminal {@link Task}s.<br>
     *
     * @param workPath contains all {@link Task}s to be consumed but ending tasks
     */
    private WorkPath processIntermediateTasks(WorkPath workPath) {
        log.info("Processing {} 'intermediate' tasks", workPath.getTasksToProcess().size());
        /* For the sake of readability */
        var tasksToProcess = workPath.getTasksToProcess();
        Set<Task> tasksToRemoveFromMap = new HashSet<>();
        while (!workPath.onlyEndingTaskRemains()) {
            tasksToProcess.stream()
                    .filter(Task.isTerminalTask.negate())
                    .filter(Task.hasAllItsNecessaryInputFluxes)
                    .forEach(currentTask -> {
                        Flux<?> flux = currentTask.process(this.removeOptional.andThen(this.convertCollectionToArray)
                                .apply(currentTask.getInputFluxesMap().values()));
                        currentTask.getSuccessors().forEach(nextTask -> workPath.injectFlux(currentTask, nextTask, flux));
                        tasksToRemoveFromMap.add(currentTask);
                    });
            tasksToRemoveFromMap.forEach(tasksToProcess::remove);
            tasksToRemoveFromMap.clear();
        }
        log.info("Done");
        return workPath;
    }

    /**
     * FinalTasks (or TerminalTasks) are the {@link Task}s we want to compute the resulting {@link Flux}.<br>
     */
    private WorkPath processFinalTasks(WorkPath workPath) {
        log.info("Processing 'terminal' task {}", workPath.getEndingTask().getName());
        workPath.getTasksToProcess().forEach(currentTask -> {
            Flux<?> flux = currentTask.process(this.removeOptional.andThen(this.convertCollectionToArray)
                    .apply(currentTask.getInputFluxesMap().values()));
            flux.log().subscribe(log::info);
        });
        log.info("Done");
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
