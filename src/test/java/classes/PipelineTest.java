package classes;

import com.citizenweb.tooling.taskpipeline.model.Operation;
import com.citizenweb.tooling.taskpipeline.model.Pipeline;
import com.citizenweb.tooling.taskpipeline.model.Task;
import data.DataForTests;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;

@Slf4j
public class PipelineTest {

    static { DataForTests.initData(); }
    private static final Map<String, Operation> operationsMap = DataForTests.getData();

    @Test
    void pipelineOfTwoWorkPaths_1() {
        Task t1 = new Task("Count to 10", operationsMap.get("Count to 10"), Collections.emptyList());
        Task t2 = new Task("Count to 100", operationsMap.get("Count to 100"), Collections.emptyList());
        Task t3 = new Task("Reverse count to 0", operationsMap.get("Reverse count to 0"), Collections.emptyList());
        Task t4 = new Task("Sum t1 t2", operationsMap.get("Sum"), List.of(t1, t2));
        Task t5 = new Task("Sum t2 t3", operationsMap.get("Sum"), List.of(t2, t3));
        Task t6 = new Task("Sum t4 t5", operationsMap.get("Sum"), List.of(t4, t5));
        Set<Task> allTasks = Set.of(t1, t2, t3, t4, t5, t6);
        Pipeline pipeline = new Pipeline(allTasks);
        var resultMap = pipeline.execute();
        resultMap.forEach((name, future) -> future.join());
        this.printTasksState.accept(pipeline);
    }

    @Test
    void pipelineOfTwoWorkPaths_2() {
        Task t3 = new Task("Reverse count to 0", operationsMap.get("Reverse count to 0"), Collections.emptyList());
        Task t7 = new Task("Send word", operationsMap.get("Send word"), Collections.emptyList());
        Task t8 = new Task("Create sentence", operationsMap.get("Create sentence"), List.of(t3, t7));
        Set<Task> allTasks = Set.of(t3, t7, t8);
        Pipeline pipeline = new Pipeline(allTasks);
        var resultMap = pipeline.execute();
        resultMap.forEach((name, future) -> future.join());
        this.printTasksState.accept(pipeline);
    }

    @Test
    void pipelineOfTwoWorkPaths_DEFAULT_OPTIMIZER() {
        Task t1 = new Task("Count to 10", operationsMap.get("Count to 10"), Collections.emptyList());
        Task t2 = new Task("Count to 100", operationsMap.get("Count to 100"), Collections.emptyList());
        Task t3 = new Task("Reverse count to 0", operationsMap.get("Reverse count to 0"), Collections.emptyList());
        Task t4 = new Task("Sum t1 t2", operationsMap.get("Sum"), List.of(t1, t2));
        Task t5 = new Task("Sum t2 t3", operationsMap.get("Sum"), List.of(t2, t3));
        Task t6 = new Task("Sum t4 t5", operationsMap.get("Sum"), List.of(t4, t5));
        Task t7 = new Task("Send word", operationsMap.get("Send word"), Collections.emptyList());
        Task t8 = new Task("Create sentence", operationsMap.get("Create sentence"), List.of(t3, t7));
        Task t9 = new Task("Top task", operationsMap.get("Top task"), List.of(t6, t8));
        Set<Task> allTasks = Set.of(t1, t2, t3, t4, t5, t6, t7, t8, t9);
        Pipeline pipeline = new Pipeline(allTasks);
        var resultMap = pipeline.execute();
        resultMap.forEach((name, future) -> future.join());
        this.printTasksState.accept(pipeline);
    }

    @Test
    void listAndLinkedHashSetCompatibility() {
        List<String> stringList = List.of("zozo", "alter", "barman");
        LinkedHashSet<String> stringLinkedHashSet = new LinkedHashSet<>(stringList);
        Assertions.assertArrayEquals(new String[]{"zozo", "alter", "barman"}, stringLinkedHashSet.toArray());
    }

    private final Consumer<Pipeline> printTasksState = pipeline -> {
        log.info("--- MONITOR DATA ---");
        pipeline.getTasks().forEach(task -> log.info(String.format("Task %s -> %s", task.getName(), task.getMonitor())));
    };


}
