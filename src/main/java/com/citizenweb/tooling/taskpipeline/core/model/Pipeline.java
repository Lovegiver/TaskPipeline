package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.ProcessingType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link Pipeline} contains all the logic needed to consume {@link Task}s in the most efficient way
 */
@Slf4j
public class Pipeline extends Monitorable {
    /**
     * All the {@link Task} to process
     */
    @Getter
    private final Set<Task> tasks;

    /**
     * The {@link PathOptimizer} will organize all {@link Task}s into {@link WorkPath}.<br>
     * It is possible to define a custom optimizer and inject it into the {@link Pipeline} with constructor.
     */
    private final PathOptimizer optimizer;

    @Getter
    private Collection<WorkPath> workPaths;

    /** Pipeline's execution results in producing {@link CompletableFuture} */
    private final ConcurrentHashMap<String, CompletableFuture<?>> runningWorkPaths = new ConcurrentHashMap<>();

    public Pipeline(String name, Set<Task> tasksToProcess) {
        super(new Monitor(ProcessingType.PIPELINE), Objects.requireNonNull(name, "A Pipeline has to be named"));
        super.setNotifier(new StateNotifier(this));
        this.tasks = tasksToProcess;
        this.optimizer = PathOptimizer.DEFAULT_OPTIMIZER;
    }

    public Pipeline(String name, Set<Task> tasksToProcess, PathOptimizer optimizer) {
        super(new Monitor(ProcessingType.PIPELINE), Objects.requireNonNull(name, "A Pipeline has to be named"));
        super.setNotifier(new StateNotifier(this));
        this.tasks = tasksToProcess;
        this.optimizer = optimizer;
    }

    /**
     * From the given tasks, compute all possible paths, ie all the tasks to process
     * in order to complete a 'terminal' (final, ending) {@link Task}
     */
    public Map<String, CompletableFuture<?>> execute() {
        super.monitor.statusToRunning();
        this.workPaths = this.optimizer.optimize(this.tasks);
        this.propagatePipeline();
        super.notifier.notifyStateChange();
        log.info("Found {} work paths", workPaths.size());
        this.workPaths.parallelStream().forEach(workPath -> {
            CompletableFuture<?> future = workPath.execute();
            runningWorkPaths.put(workPath.getName(), future);
        });
        super.getMonitor().statusToDone();
        super.getNotifier().notifyStateChange();
        return this.runningWorkPaths;
    }

    /**
     * Once the Pipeline is instantiated, it will compute possible {@link WorkPath}s thanks to its {@link PathOptimizer}.<br>
     * Then, all objects are known : the pipeline, its workpaths and all tasks within each workpath.
     * Each workpath and task must know about their wrapping pipeline in order to trigger state export thanks to
     * their {@link Notifier}.
     */
    private void propagatePipeline() {
        this.workPaths.forEach(workPath -> workPath.setNotifier(new StateNotifier(this)));
        this.tasks.forEach(task -> task.setNotifier(new StateNotifier(this)));
    }

}
