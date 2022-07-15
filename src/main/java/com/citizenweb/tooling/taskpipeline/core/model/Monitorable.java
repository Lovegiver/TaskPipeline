package com.citizenweb.tooling.taskpipeline.core.model;

import lombok.*;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

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

    @Getter @Setter
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Pipeline parent;
    /** All the necessary input fluxes are ready to use */
    public static Predicate<Monitorable> hasAllItsNecessaryInputFluxes = monitorable -> monitorable.getInputFluxesMap().values().stream()
            .allMatch(Optional::isPresent);

    protected Monitorable(@NonNull Monitor monitor, @NonNull String name) {
        this.monitor = monitor;
        this.name = name;
    }

}
