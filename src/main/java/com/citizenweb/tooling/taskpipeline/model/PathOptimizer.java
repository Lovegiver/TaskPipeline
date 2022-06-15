package com.citizenweb.tooling.taskpipeline.model;

import java.util.*;
import java.util.stream.Collectors;


@FunctionalInterface
public interface PathOptimizer {

    /**
     * What is a <b>path</b> ?<br>
     * A <b>path</b> is a collection of {@link Task}s, all of them are linked to each other by a previous/next relationship.<br>
     * Starting from a collection of tasks, we want to find out all the different paths.<br>
     *
     * @param allTasks the whole collection of tasks
     * @return a collection of {@link WorkPath}
     */
    Collection<WorkPath>  optimize(Set<Task> allTasks);

    /**
     * DEFAULT implementation of {@link PathOptimizer}
     */
    PathOptimizer DEFAULT_OPTIMIZER = allTasks -> {
        List<WorkPath> workPaths = new ArrayList<>();
        Set<Task> terminalTasks = allTasks.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        for (Task task : terminalTasks) {
            Set<Task> pathForTask = new HashSet<>();
            findAllTasksFromTree(pathForTask, task);
            workPaths.add(new WorkPath(pathForTask));
        }
        return workPaths;
    };

    /**
     * Recursive function used to compute the whole path associated to a terminal {@link Task}.<br>
     *
     * @param path the path to build
     * @param task the reference {@link Task}
     */
    private static void findAllTasksFromTree(Set<Task> path, Task task) {
        path.add(task);
        if (!Task.isInitialTask.test(task)) {
            for (Task t : task.getPredecessors()) {
                findAllTasksFromTree(path, t);
            }
        }
    }

}
