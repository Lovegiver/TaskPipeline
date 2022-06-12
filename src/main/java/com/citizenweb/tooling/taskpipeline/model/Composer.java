package com.citizenweb.tooling.taskpipeline.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public abstract class Composer implements Operation {

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
    /** All the necessary input fluxes are ready to use */
    public static Predicate<Composer> hasAllItsNecessaryInputFluxes = composer -> composer.getInputFluxesMap().values().stream()
            .allMatch(Optional::isPresent);

    protected Composer(@NonNull Monitor monitor, @NonNull String name) {
        this.monitor = monitor;
        this.name = name;
    }

    /**
     * Processes the input {@link Flux} with any logic you deserve and then return a Flux.<br>
     *
     * @param inputs the Flux coming from preceding Operations
     * @return a Flux to be used by next Operation-s or to be subscribed to in order to finally get the result
     */
    @Override
    abstract public Flux<?> process(Flux<?>... inputs);
}
