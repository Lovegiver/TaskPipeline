package com.citizenweb.tooling.taskpipeline.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import reactor.core.publisher.Flux;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

@Data
public class Task implements Operation {

    private final String taskName;
    private final Operation wrappedOperation;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> predecessors;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Set<Task> successors = new HashSet<>();

    public static Predicate<Task> isTerminalTask = task -> task.getSuccessors().isEmpty();
    public static Predicate<Task> isInitialTask = task -> task.getPredecessors().isEmpty();

    public Task(String taskName, Operation wrappedOperation, Set<Task> predecessors) {
        this.taskName = taskName;
        this.wrappedOperation = wrappedOperation;
        this.predecessors = predecessors;
        this.predecessors.forEach(p -> p.getSuccessors().add(this));
    }

    @Override
    public Flux<?> process(Flux<?>... inputs) {
        return this.wrappedOperation.process(inputs);
    }
}
