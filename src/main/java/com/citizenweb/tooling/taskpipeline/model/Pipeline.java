package com.citizenweb.tooling.taskpipeline.model;

import reactor.core.publisher.Flux;
import reactor.util.annotation.NonNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    Function<Collection<Flux<?>>,Flux<?>[]> convertCollectionToArray = collection -> {
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };

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

    private void processStartingTasks(Collection<Task> startingTasks) {
        startingTasks.forEach(task -> {
            Flux<?> flux = task.process(Flux.empty());
            task.getSuccessors().forEach(next -> this.injectFluxIntoNextTask.accept(next, flux));
        });
    }

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

    private void processFinalTasks() {
        this.tasksRelationships.forEach((key, value) -> {
            Flux<?> flux = key.process(this.convertCollectionToArray.apply(value));
            flux.log().subscribe(System.out::println);
        });
    }

    void findAllTasksFromTree(Set<Task> path, Task task) {
        path.add(task);
        if (!Task.isInitialTask.test(task)) {
            for (Task t : task.getPredecessors()) {
                this.findAllTasksFromTree(path, t);
            }
        }
    }

}
