package com.citizenweb.tooling.taskpipeline.model;

import lombok.Data;
import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A work path is a set of tasks that all are involved in the realization of an ending task.
 */
public class WorkPath {
    @Getter
    private final String name;
    @Getter
    private final Set<Task> tasks;
    @Getter
    private final Set<Task> startingTasks;
    @Getter
    private final Task endingTask;
    /** For a given {@link Task}, associated values are the input fluxes needed to execute the {@link Operation#process(Flux[])}. */
    private final Map<Task, Collection<Flux<?>>> tasksRelationships = new ConcurrentHashMap<>();

    public WorkPath(Set<Task> taskToProcess) {
        this.tasks = taskToProcess;
        this.startingTasks = taskToProcess.stream().filter(Task.isInitialTask).collect(Collectors.toSet());
        this.endingTask = taskToProcess.stream().filter(Task.isTerminalTask).findAny().orElseThrow();
        this.name = taskToProcess.stream().filter(Task.isTerminalTask).map(Task::getTaskName).findAny().orElseThrow();
    }
}