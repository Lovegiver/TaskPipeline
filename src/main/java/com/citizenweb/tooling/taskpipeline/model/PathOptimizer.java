package com.citizenweb.tooling.taskpipeline.model;

import com.citizenweb.tooling.taskpipeline.utils.TaskUtils;

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
    Collection<WorkPath> optimize(Set<Task> allTasks);

    /**
     * DEFAULT implementation of {@link PathOptimizer}
     */
    PathOptimizer DEFAULT_OPTIMIZER = allTasks -> {
        List<WorkPath> workPaths = new ArrayList<>();
        Set<Task> terminalTasks = allTasks.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        for (Task task : terminalTasks) {
            Set<Task> pathFromTerminalTask = new HashSet<>();
            TaskUtils.buildPathFromTerminalToInitial(pathFromTerminalTask, task, 0);
            workPaths.add(new WorkPath(pathFromTerminalTask));
        }
        return workPaths;
    };

}
