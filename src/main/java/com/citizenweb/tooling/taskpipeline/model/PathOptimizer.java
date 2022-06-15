package com.citizenweb.tooling.taskpipeline.model;

import java.util.Set;

@FunctionalInterface
public interface PathOptimizer {
    Set<? super WorkPath> optimize(Set<? super Task> allTasks);
}
