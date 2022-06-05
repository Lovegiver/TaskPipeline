package com.citizenweb.tooling.taskpipeline.model;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A work path is a set of tasks that all are involved in the realization of an ending task.
 */
@Log4j2
public class WorkPath {
    @Getter
    private final String name;
    @Getter
    private final Set<Task> tasks;
    @Getter
    private final Set<Task> startingTasks;
    @Getter
    private final Task endingTask;
    /**
     * Staging collection for {@link Task}s waiting for being processed.
     */
    @Getter
    private final Set<Task> tasksToProcess = ConcurrentHashMap.newKeySet();

    public WorkPath(Set<Task> taskToProcess) {
        this.tasks = taskToProcess;
        this.startingTasks = taskToProcess.stream().filter(Task.isInitialTask).collect(Collectors.toSet());
        this.endingTask = taskToProcess.stream().filter(Task.isTerminalTask).findAny().orElseThrow();
        this.name = taskToProcess.stream().filter(Task.isTerminalTask).map(Task::getTaskName).findAny().orElseThrow();
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
