package com.citizenweb.tooling.taskpipeline.model;

import com.citizenweb.tooling.taskpipeline.exceptions.TaskExecutionException;
import com.citizenweb.tooling.taskpipeline.utils.ProcessingType;
import lombok.*;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.function.BiConsumer;
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
@Log4j2
@EqualsAndHashCode(callSuper = true)
public class Task extends Composer {

    /**
     * The wrapped {@link Operation} - Mandatory
     */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final Operation wrappedOperation;
    /**
     * All {@link Task}s to be executed <b>before</b> the current one (inputs for current Task)
     */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final LinkedHashSet<Task> predecessors;
    /**
     * All {@link Task}s to be executed <b>after</b> the current one (current Task is an input for them)
     */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final LinkedHashSet<Task> successors = new LinkedHashSet<>();

    /**
     * This {@link Task} has no <b>successors</b>.
     */
    public static Predicate<Task> isTerminalTask = task -> task.getSuccessors().isEmpty();
    /**
     * This {@link Task} has no <b>predecessors</b>.
     */
    public static Predicate<Task> isInitialTask = task -> task.getPredecessors().isEmpty();
    /** All the necessary input fluxes are ready to use */
    public static Predicate<Task> hasAllItsNecessaryInputFluxes = task -> task.getInputFluxesMap().values().stream()
            .allMatch(Optional::isPresent);

    /**
     * Important note : the previous {@link Task}s are expressed within a {@link List} because <b>order</b>
     * is important.<br>
     * The order of the previous {@link Task}s <b>must</b> be the same as the {@link Operation}"s input {@link Flux}es.<br>
     * @param taskName the name of the current {@link Task} for {@link Monitor}ing and logging
     * @param wrappedOperation the {@link Operation} wrapped by this {@link Task}
     * @param predecessors {@link Task}s to be executed before the current one
     */
    public Task(String taskName, Operation wrappedOperation, List<Task> predecessors) {
        super(new Monitor(ProcessingType.TASK), Objects.requireNonNull(taskName,
                "A Task has to be named"));
        this.wrappedOperation = Objects.requireNonNull(wrappedOperation,
                "A Task should wrap an Operation, but Operation is missing");
        this.predecessors = new LinkedHashSet<>(Objects.requireNonNull(predecessors,
                "Null is not an acceptable value. Consider using an empty collection."));
        this.predecessors.forEach(p -> {
            p.getSuccessors().add(this);
            this.inputFluxesMap.put(p, Optional.empty());
        });
    }

    /**
     * Executes the wrapped {@link Operation}.<br>
     * @param inputs the Flux coming from preceding Operations
     * @return a output {@link Flux}
     */
    @Override
    public Flux<?> process(Flux<?>... inputs) {
        try {
            this.monitor.statusToRunning();
            Flux<?> outputFlux = this.wrappedOperation.process(inputs);
            this.monitor.statusToDone();
            return outputFlux;
        } catch (Exception ex) {
            this.monitor.statusToError();
            String taskSignature = String.format("%s / %s", this.getName(), this.monitor.getId());
            throw new TaskExecutionException(getErrorMessage(ex, taskSignature));
        }
    }

    /**
     * Once a 'predecessor' has produced its output {@link Flux}, we can replace the default {@link Optional#empty()}
     * in the {@link Task#inputFluxesMap} of the consuming {@link Task}.<br>
     */
    public BiConsumer<Task,Flux<?>> injectFluxFromTask = ((task, flux) ->
            this.inputFluxesMap.replace(task, Optional.empty(), Optional.of(flux)));

    /**
     * Retrieve the root cause of an exception :
     * <ol>
     *     <li>primarily with the {@link Throwable#getCause()}</li>
     *     <li>else with the {@link Throwable#getMessage()}</li>
     * </ol>
     * @param throwable the thrown exception we want to get message from
     * @param taskName the involved task to enrich the returned message
     * @return a String composed by the exception root cause and the {@link Task}'s name
     */
    private String getErrorMessage(Throwable throwable, String taskName) {
        String errMsg = throwable.getCause() != null ? throwable.getCause().toString() : throwable.getMessage();
        return String.format("Exception while processing task [ %s ] -> %s", taskName, errMsg);
    }
}
