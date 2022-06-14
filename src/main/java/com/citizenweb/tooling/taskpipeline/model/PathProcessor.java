package com.citizenweb.tooling.taskpipeline.model;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface PathProcessor {
    CompletableFuture<?> process(Collection<? super WorkPath> tasksToProcess);
}
