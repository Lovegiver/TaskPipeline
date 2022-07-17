package com.citizenweb.tooling.taskpipeline.core.model;

import lombok.*;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * A {@link Monitorable} contains a {@link Monitor} field through which we can monitor
 * the object's state.<br>
 * {@link Pipeline}, {@link WorkGroup} and {@link Task} are monitorable objects.
 */
public abstract class Monitorable {

    /** Monitors life cycle */
    @NonNull
    @Getter
    protected final Monitor monitor;
    /**
     * Task's name - Mandatory
     */
    @NonNull
    @Getter
    private final String name;
    /** Each time a 'predecessor' produces a {@link Flux}, it has to be injected in the right order for further execution */
    @NonNull
    @Getter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    protected final Map<Task, Optional<Flux<?>>> inputFluxesMap = Collections.synchronizedMap(new LinkedHashMap<>());
    /** The {@link Notifier} will be used each time the object's state changes */
    @Getter @Setter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    protected Notifier notifier;
    /** All the necessary input fluxes are ready to use */
    public static Predicate<Monitorable> hasAllItsNecessaryInputFluxes = monitorable ->
            monitorable.getInputFluxesMap().values().stream().allMatch(Optional::isPresent);

    protected Monitorable(@NonNull Monitor monitor, @NonNull String name) {
        this.monitor = monitor;
        this.name = name;
    }

}
