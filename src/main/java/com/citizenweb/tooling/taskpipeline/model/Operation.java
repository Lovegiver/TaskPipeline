package com.citizenweb.tooling.taskpipeline.model;

import reactor.core.publisher.Flux;

@FunctionalInterface
public interface Operation {
    Flux<?> process(Flux<?>... inputs);
}
