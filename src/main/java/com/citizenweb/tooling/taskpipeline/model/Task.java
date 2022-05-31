package com.citizenweb.tooling.taskpipeline.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import reactor.core.publisher.Flux;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A {@link Task} is a wrapper for an {@link Operation} object.<br>
 * To be useful, an Operation need a Task around it. In fact, a Task could live without Operation, holding all
 * the logic in itself. But it seems a better idea to have two distinct objects for the sake of ease-of-use.<br>
 * First the Operation is defined, with only the logic. Then a Task is instantiated with an Operation  as a mandatory
 * argument in its constructor.<br>
 * A collection containing previous {@link Task}s is also mandatory, but this collection can be empty if the Task wraps
 * a 'starting' operation.
 */
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
