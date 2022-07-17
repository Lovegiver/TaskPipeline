package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.TaskUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link WorkGroupOptimizer} computes all possible {@link WorkGroup}s starting from a set of tasks.<br>
 * We provide a {@link #DEFAULT_OPTIMIZER} that will create one {@link WorkGroup} by 'terminal' {@link Task}.<br>
 * You can provide your own {@link WorkGroupOptimizer} when instantiating a new {@link Pipeline}.
 */
@FunctionalInterface
public interface WorkGroupOptimizer {

    /**
     * What is a <b>{@link WorkGroup}</b> ?<br>
     * A <b>WorkGroup</b> is a collection of {@link Task}s, all of them are linked to each other by a previous/next relationship.<br>
     * Starting from a collection of tasks, we want to find out all the different workgroups.<br>
     *
     * @param allTasks the whole collection of tasks
     * @return a collection of {@link WorkGroup}s
     */
    Collection<WorkGroup> optimize(Set<Task> allTasks);

    /**
     * DEFAULT implementation of {@link WorkGroupOptimizer}
     */
    WorkGroupOptimizer DEFAULT_OPTIMIZER = allTasks -> {
        List<WorkGroup> workGroups = new ArrayList<>();
        Set<Task> terminalTasks = allTasks.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        for (Task task : terminalTasks) {
            Set<Task> workgroupFromTerminalTask = new HashSet<>();
            TaskUtils.buildWorkGroupFromTerminalToInitial(workgroupFromTerminalTask, task, 0);
            workGroups.add(new WorkGroup(workgroupFromTerminalTask));
        }
        return workGroups;
    };

}
