package com.citizenweb.tooling.taskpipeline.model;

import lombok.Data;
import lombok.Getter;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * A work path is a set of tasks that all are involved in the realization of an ending task.
 */
public class WorkPath {
    @Getter
    private final Set<Task> tasks;
    @Getter
    private final Set<Task> startingTasks;
    @Getter
    private final Set<Task> endingTasks;

    public WorkPath(Set<Task> taskToProcess) {
        this.tasks = taskToProcess;
        this.startingTasks = taskToProcess.stream().filter(task -> task.getPredecessors().isEmpty()).collect(Collectors.toSet());
        this.endingTasks = taskToProcess.stream().filter(task -> task.getSuccessors().isEmpty()).collect(Collectors.toSet());
    }
}
