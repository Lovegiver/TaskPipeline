package com.citizenweb.tooling.taskpipeline.utils;

import com.citizenweb.tooling.taskpipeline.model.Task;

import java.util.Set;

public class TaskUtils {

    /**
     * Recursive function used to compute the whole path associated to a terminal {@link Task}.<br>
     *
     * @param path the path to build
     * @param task the reference {@link Task}
     * @param rank position of the task into the work path (default : terminal task = 1)
     */
    public static void buildPathFromTerminalToInitial(Set<Task> path, Task task, int rank) {
        int taskRank = rank + 1;
        task.getMonitor().setRank(taskRank);
        path.add(task);
        if (!Task.isInitialTask.test(task)) {
            task.getPredecessors().forEach(predecessor -> buildPathFromTerminalToInitial(path, predecessor, taskRank));
        }
    }

}
