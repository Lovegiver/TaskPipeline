package com.citizenweb.tooling.taskpipeline.model;

import lombok.*;
import reactor.core.publisher.Flux;

import java.util.HashSet;
import java.util.Objects;
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
@Builder
public class Task implements Operation {

    /** Task's name - Mandatory */
    @NonNull
    private final String taskName;
    /** The wrapped {@link Operation} - Mandatory */
    @NonNull
    private final Operation wrappedOperation;
    /** All {@link Task}s to be executed <b>before</b> the current one (inputs for current Task) */
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @NonNull
    private final Set<Task> predecessors;
    /** All {@link Task}s to be executed <b>after</b> the current one (current Task is an input for them) */
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @NonNull
    private final Set<Task> successors = new HashSet<>();

    /** This {@link Task} has no <b>successors</b>. */
    public static Predicate<Task> isTerminalTask = task -> task.getSuccessors().isEmpty();
    /** This {@link Task} has no <b>predecessors</b>. */
    public static Predicate<Task> isInitialTask = task -> task.getPredecessors().isEmpty();

    public Task(String taskName, Operation wrappedOperation, Set<Task> predecessors) {
        this.taskName = Objects.requireNonNull(taskName,
                "A Task has to be named");
        this.wrappedOperation = Objects.requireNonNull(wrappedOperation,
                "A Task should wrap an Operation, but Operation is missing");
        this.predecessors = Objects.requireNonNull(predecessors,
                "Null is not an acceptable value. Consider using an empty collection.");
        this.predecessors.forEach(p -> p.getSuccessors().add(this));
    }

    @Override
    public Flux<?> process(Flux<?>... inputs) {
        return this.wrappedOperation.process(inputs);
    }
}
