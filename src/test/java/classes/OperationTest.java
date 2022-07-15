package classes;

import com.citizenweb.tooling.taskpipeline.core.model.Operation;
import com.citizenweb.tooling.taskpipeline.core.model.Pipeline;
import com.citizenweb.tooling.taskpipeline.core.model.Task;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class OperationTest {

    private static final Map<String, Operation> operationsMap = new HashMap<>();

    @BeforeAll
    static void initData() {
        Operation o1 = inputs -> Flux.just(5, 6, 7);
        Operation o2 = inputs -> Flux.just(7, 7, 7);
        Operation o3 = inputs -> Flux.just(15, 15, 15);
        Operation o4 = inputs -> {
            Flux<?> int1 = inputs[0];
            Flux<?> int2 = inputs[1];
            return Flux.zip(int1, int2, (x, y) -> (int) x + (int) y);
        };
        operationsMap.put("o1", o1);
        operationsMap.put("o2", o2);
        operationsMap.put("o3", o3);
        operationsMap.put("o4", o4);
    }

    Function<Collection<Flux<?>>, Flux<?>[]> convertCollectionToArray = collection -> {
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };

    @SuppressWarnings("unchecked")
    @Test
    void simpleOperationsTest_noPipeline() {
        Task t1 = new Task("Task 1", operationsMap.get("o1"), Collections.emptyList());
        Task t2 = new Task("Task 2", operationsMap.get("o2"), Collections.emptyList());
        Task t3 = new Task("Task 3", operationsMap.get("o3"), Collections.emptyList());
        Task t4 = new Task("Task 4", operationsMap.get("o4"), List.of(t1, t2));
        Task t5 = new Task("Task 5", operationsMap.get("o4"), List.of(t2, t3));
        Task t6 = new Task("Task 6", operationsMap.get("o4"), List.of(t4, t5));

        Set<Task> completePath = Set.of(t1, t2, t3, t4, t5, t6);

        Set<Task> endingTasks = completePath.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        Set<Task> startingTasks = completePath.stream().filter(Task.isInitialTask).collect(Collectors.toSet());

        Map<Task, Collection<Flux<?>>> tasksRelationships = new ConcurrentHashMap<>();

        BiConsumer<Task, Flux<?>> injectFluxIntoNextTask = ((next, flux) -> {
            if (tasksRelationships.get(next) != null) {
                tasksRelationships.get(next).add(flux);
            } else {
                Set<Flux<?>> fluxSet = new HashSet<>();
                fluxSet.add(flux);
                tasksRelationships.put(next, fluxSet);
            }
        });

        for (Task task : startingTasks) {
            Flux<?> flux = task.getWrappedOperation().process(Flux.empty());
            task.getSuccessors().forEach(next -> injectFluxIntoNextTask.accept(next, flux));
        }

        while (!endingTasks.containsAll(tasksRelationships.keySet())) {
            Set<Task> tasksToRemoveFromMap = new HashSet<>();
            for (Task task : tasksRelationships.keySet()) {
                Flux<?> flux = task.getWrappedOperation().process(convertCollectionToArray.apply(tasksRelationships.get(task)));
                task.getSuccessors().forEach(next -> injectFluxIntoNextTask.accept(next, flux));
                tasksToRemoveFromMap.add(task);
            }
            tasksToRemoveFromMap.forEach(tasksRelationships::remove);
            tasksToRemoveFromMap.clear();
        }

        tasksRelationships.forEach((key, value) -> {
            Flux<Integer> flux = (Flux<Integer>) key.getWrappedOperation().process(convertCollectionToArray.apply(value));
            flux.log().subscribe(o -> log.info(String.valueOf(o)));

            StepVerifier.create(flux)
                    .expectSubscription()
                    .expectNext(34, 35, 36)
                    .expectComplete()
                    .verify();
        });

    }

    @Test
    void simpleOperationsTest_withPipeline() {
        Task t1 = new Task("Task 1", operationsMap.get("o1"), Collections.emptyList());
        Task t2 = new Task("Task 2", operationsMap.get("o2"), Collections.emptyList());
        Task t3 = new Task("Task 3", operationsMap.get("o3"), Collections.emptyList());
        Task t4 = new Task("Task 4", operationsMap.get("o4"), List.of(t1, t2));
        Task t5 = new Task("Task 5", operationsMap.get("o4"), List.of(t2, t3));
        Task t6 = new Task("Task 6", operationsMap.get("o4"), List.of(t4, t5));
        Set<Task> allTasks = Set.of(t1, t2, t3, t4, t5, t6);
        Pipeline pipeline = new Pipeline("Pipeline", allTasks);
        var resultMap = pipeline.execute();
        resultMap.forEach((key, value) -> value.join());
    }

}
