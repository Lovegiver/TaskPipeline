package com.citizenweb.tooling.taskpipeline.model;

import com.citizenweb.tooling.taskpipeline.utils.ProcessingType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
