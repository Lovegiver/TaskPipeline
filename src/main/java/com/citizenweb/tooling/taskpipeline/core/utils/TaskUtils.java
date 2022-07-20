package com.citizenweb.tooling.taskpipeline.core.utils;

import com.citizenweb.tooling.taskpipeline.core.model.Task;

import java.util.Set;

public class TaskUtils {

    /**
     * Recursive function used to compute the whole workgroup associated to a terminal {@link Task}.<br>
     *
     * @param workgroup the workgroup to build
     * @param task the reference {@link Task}
     * @param rank position of the task into the work workgroup (default : terminal task = 1)
     */
    public static void buildWorkGroupFromTerminalToInitial(Set<Task> workgroup, Task task, int rank) {
        int taskRank = rank + 1;
        task.getMonitor().setRank(taskRank);
        workgroup.add(task);
        if (!Task.isInitialTask.test(task)) {
            task.getPredecessors().forEach(predecessor -> buildWorkGroupFromTerminalToInitial(workgroup, predecessor, taskRank));
        }
    }

}
