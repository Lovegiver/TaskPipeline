package com.citizenweb.tooling.taskpipeline.model;

import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class Pipeline {
    /** For a given {@link Task}, associated values are the input fluxes needed to execute the {@link Operation#process(Flux[])}. */
    private final Map<Task, Collection<Flux<?>>> tasksRelationships = new ConcurrentHashMap<>();
    /** All the {@link Task} to process */
    private final Set<Task> tasks;

    public Pipeline(Set<Task> tasksToProcess) {
        this.tasks = tasksToProcess;
    }

    BiConsumer<Task,Flux<?>> injectFluxIntoNextTask = ((next, flux) -> {
        if (tasksRelationships.get(next) != null) {
            tasksRelationships.get(next).add(flux);
        } else {
            Set<Flux<?>> fluxSet = new HashSet<>();
            fluxSet.add(flux);
            tasksRelationships.put(next, fluxSet);
        }
    });

    public void execute() {

    }

}
