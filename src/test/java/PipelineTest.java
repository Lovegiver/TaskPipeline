import com.citizenweb.tooling.taskpipeline.model.Operation;
import com.citizenweb.tooling.taskpipeline.model.Pipeline;
import com.citizenweb.tooling.taskpipeline.model.Task;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.function.Consumer;

@Slf4j
public class PipelineTest {

    private static final Map<String, Operation> operationsMap = new HashMap<>();

    @BeforeAll
    static void initData() {
        Operation o1 = inputs -> Flux.range(1,10);
        Operation o2 = inputs -> Flux.range(91,10);
        Operation o3 = inputs -> Flux.just(18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
        Operation o4 = inputs -> {
            Flux<?> int1 = inputs[0];
            Flux<?> int2 = inputs[1];
            return Flux.zip(int1, int2, (x, y) -> (int) x + (int) y);
        };
        @SuppressWarnings("SpellCheckingInspection")
        Operation o5 = inputs -> Flux.just("pangolins", "chien",
                "chat", "tomates", "hamburgers", "voitures", "maisons", "personnes", "percolateurs", "souris");
        Operation o6 = inputs -> {
            Flux<?> int1 = inputs[0];
            Flux<?> word1 = inputs[1];
            return Flux.zip(int1, word1, (i, w) -> String.format("I've just seen %d %s flying ^^", (int) i, w));
        };
        Operation o7 = inputs -> {
            Flux<?> int1 = inputs[0];
            Flux<?> word1 = inputs[1];
            return Flux.zip(int1, word1, (i, w) -> String.format("I've said [ %s ] around [ %d ] times !!", w, (int) i));
        };
        operationsMap.put("Count to 10", o1);
        operationsMap.put("Count to 100", o2);
        operationsMap.put("Reverse count to 0", o3);
        operationsMap.put("Sum", o4);
        operationsMap.put("Send word", o5);
        operationsMap.put("Create sentence", o6);
        operationsMap.put("Top task", o7);
    }

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
    void pipelineOfTwoWorkPaths_3() {
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
