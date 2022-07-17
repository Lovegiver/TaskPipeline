package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.ProcessingType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A work path is a set of tasks that all are involved in the realization of an ending task.
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class WorkPath extends Monitorable {
    /** Collection of all {@link Task}s belonging to this WorkPath */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> tasks;
    /** Collection of all INITIAL {@link Task}s : tasks to be processed first */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> startingTasks;
    /** The TERMINAL {@link Task} */
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
    /** Dedicated {@link Scheduler} */
    private final Scheduler scheduler = Schedulers.parallel();

    public WorkPath(Set<Task> taskToProcess) {
        super(new Monitor(ProcessingType.WORKPATH),
                taskToProcess.stream().map(Task::getName).collect(Collectors.joining(",")));
        this.tasks = taskToProcess;
        this.startingTasks = taskToProcess.stream().filter(Task.isInitialTask).collect(Collectors.toSet());
        this.endingTask = taskToProcess.stream().filter(Task.isTerminalTask).findAny().orElseThrow();
    }

    /**
     * Does 3 steps :
     * <ol>
     *     <li>process all starting tasks</li>
     *     <li>process all intermediate tasks</li>
     *     <li>process all terminal tasks</li>
     * </ol>
     */
    public CompletableFuture<?> execute() {
        return CompletableFuture.supplyAsync( () -> {
            super.monitor.statusToRunning();
            super.notifier.notifyStateChange();
            return this.processStartingTasks();
                })
                .thenApply(this::processIntermediateTasks)
                .thenApply(this::processFinalTasks)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Error occurred : " + ex.getCause());
                        super.monitor.statusToError();
                        super.notifier.notifyStateChange();
                    } else {
                        log.info("Finished processing 'work path' : " + this);
                        super.monitor.statusToDone();
                        super.notifier.notifyStateChange();
                    }
                });
    }

    /**
     * StartingTasks are all {@link Task}s without any predecessors (or previous Task).<br>
     * These {@link Task}s are specific because they do not need any input {@link Flux}.<br>
     * They all will be consumed by the following {@link Task}s.<br>
     *
     * @return a {@link WorkPath}
     */
    private WorkPath processStartingTasks() {
        log.info("Processing {} 'starting' tasks", this.getStartingTasks().size());
        this.getStartingTasks().forEach(currentTask -> {
            Flux<?> flux = currentTask.process(Flux.empty()).publishOn(this.scheduler);
            currentTask.getSuccessors()
                    .stream()
                    .filter(this::taskBelongsToWorkPath)
                    .forEach(nextTask -> this.injectFlux(currentTask, nextTask, flux));
        });
        log.info("Done");
        return this;
    }

    /**
     * IntermediateTasks have predecessors and successors.<br>
     * They all will be consumed, layer after layer, until we reach the terminal {@link Task}s.<br>
     *
     * @param workPath this object, a wrapper for all tasks dedicated to one single 'final' {@link Task}
     */
    private WorkPath processIntermediateTasks(WorkPath workPath) {
        log.info("Processing {} 'intermediate' tasks", this.getTasksToProcess().size());
        /* For the sake of readability */
        var tasksToProcess = this.getTasksToProcess();
        if (!CollectionUtils.isEmpty(tasksToProcess)) {
            Set<Task> tasksToRemoveFromMap = new HashSet<>();
            while (!this.onlyEndingTaskRemains()) {
                tasksToProcess.stream()
                        .filter(Task.isTerminalTask.negate())
                        .filter(hasAllItsNecessaryInputFluxes)
                        .forEach(currentTask -> {
                            Flux<?> flux = currentTask.process(this.removeOptional.andThen(this.convertCollectionToArray)
                                    .apply(currentTask.getInputFluxesMap().values())).publishOn(this.scheduler);
                            currentTask.getSuccessors().forEach(nextTask -> this.injectFlux(currentTask, nextTask, flux));
                            tasksToRemoveFromMap.add(currentTask);
                        });
                tasksToRemoveFromMap.forEach(tasksToProcess::remove);
                tasksToRemoveFromMap.clear();
            }
        }
        log.info("Done");
        return this;
    }

    /**
     * FinalTasks (or TerminalTasks) are the {@link Task}s we want to compute the resulting {@link Flux}.<br>
     */
    private WorkPath processFinalTasks(WorkPath workPath) {
        log.info("Processing 'terminal' task {}", this.getEndingTask().getName());
        /* For the sake of readability */
        var tasksToProcess = this.getTasksToProcess();
        if (!CollectionUtils.isEmpty(tasksToProcess)) {
            tasksToProcess.forEach(currentTask -> {
                Flux<?> flux = currentTask.process(this.getComputedInputFluxes.apply(currentTask));
                flux.log().subscribe(o -> log.info(String.valueOf(o)));
            });
        }
        log.info("Done");
        return this;
    }
    /**
     * Converts a Collection into an Array
     */
    Function<Collection<Flux<?>>, Flux<?>[]> convertCollectionToArray = collection -> {
        Objects.requireNonNull(collection, "Collection is NULL thus can't be converted into Array");
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };
    /** If any {@link Optional} of {@link Flux} are present, will convert them into plain {@link Flux} objects */
    Function<Collection<Optional<Flux<?>>>, Collection<Flux<?>>> removeOptional = optionals -> {
        Objects.requireNonNull(optionals, "Collection is NULL thus Optional can't be removed");
        Collection<Flux<?>> fluxes = new ArrayList<>(optionals.size());
        optionals.forEach(optional -> fluxes.add(optional.orElseThrow()));
        return fluxes;
    };
    /** {@link Function} composition */
    Function<Task, Flux<?>[]> getComputedInputFluxes = task -> this.removeOptional.andThen(this.convertCollectionToArray)
            .apply(task.getInputFluxesMap().values());

    /**
     * Each time a {@link Flux} is produced, we have to inject it as an input for the next {@link Task}.<br>
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
