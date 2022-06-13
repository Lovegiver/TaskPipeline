package com.citizenweb.tooling.taskpipeline.model;

import com.citizenweb.tooling.taskpipeline.utils.ProcessingType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A work path is a set of tasks that all are involved in the realization of an ending task.
 */
@Log4j2
@EqualsAndHashCode(callSuper = true)
public class WorkPath extends Wrapper {
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> tasks;
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> startingTasks;
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Task endingTask;
    /**
     * Staging collection for {@link Task}s waiting for being processed.
     */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> tasksToProcess = ConcurrentHashMap.newKeySet();

    public WorkPath(Set<Task> taskToProcess) {
        super(new Monitor(ProcessingType.WORKPATH),
                taskToProcess.stream().map(Task::getName).collect(Collectors.joining(",")));
        this.tasks = taskToProcess;
        this.startingTasks = taskToProcess.stream().filter(Task.isInitialTask).collect(Collectors.toSet());
        this.endingTask = taskToProcess.stream().filter(Task.isTerminalTask).findAny().orElseThrow();
    }


    public CompletableFuture<?> execute() {
        return CompletableFuture.supplyAsync(() -> processStartingTasks(this))
                .thenApply(this::processIntermediateTasks)
                .thenApply(this::processFinalTasks)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Error occurred : " + ex.getCause());
                    } else {
                        log.info("Finished processing 'workpath' : " + this.getName());
                    }
                });
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

    /**
     * Each time a {@link Flux} is produced, we have to inject it as an input of the next {@link Task}.<br>
     */
    public void injectFlux(Task producer, Task consumer, Flux<?> flux) {
        consumer.injectFluxFromTask.accept(producer, flux);
        this.tasksToProcess.add(consumer);
    }

    /**
     * Check the content of the {@link WorkPath#tasksToProcess} collection.<br>
     * @return TRUE if only the terminal {@link Task} remains to be processed
     */
    public boolean onlyEndingTaskRemains() {
        return this.tasksToProcess.size() == 1 && this.tasksToProcess.contains(this.endingTask);
    }

    /**
     * A {@link Task} may produce a {@link Flux} needed by other tasks not belonging to the same {@link WorkPath}.<br>
     * @param task we want to know if this {@link Task} belongs to this {@link WorkPath}
     * @return TRUE if the {@link Task} is part of this {@link WorkPath}
     */
    public boolean taskBelongsToWorkPath(Task task) {
        return this.tasks.contains(task);
    }

}
