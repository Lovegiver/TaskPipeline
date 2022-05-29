import com.citizenweb.tooling.taskpipeline.model.Operation;
import com.citizenweb.tooling.taskpipeline.model.Task;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OperationTest {

    private static final Map<String,Operation> operationsMap = new HashMap<>();

    @BeforeAll
    static void initData() {
        Operation o1 = inputs -> Flux.just(5,6,7);
        Operation o2 = inputs -> Flux.just(7,7,7);
        Operation o3 = inputs -> Flux.just(15,15,15);
        Operation o4 = inputs -> {
            Flux<?> int1 = inputs[0];
            Flux<?> int2 = inputs[1];
            return Flux.zip(int1, int2, (x,y) -> (int) x+ (int) y);
        };
        operationsMap.put("o1", o1);
        operationsMap.put("o2", o2);
        operationsMap.put("o3", o3);
        operationsMap.put("o4", o4);
    }

    @Test
    void simpleOperationsTest() {
        Task t1 = new Task("Task 1", operationsMap.get("o1"), Collections.emptySet());
        Task t2 = new Task("Task 2", operationsMap.get("o2"), Collections.emptySet());
        Task t3 = new Task("Task 3", operationsMap.get("o3"), Collections.emptySet());
        Task t4 = new Task("Task 4", operationsMap.get("o4"), Set.of(t1, t2));
        Task t5 = new Task("Task 5", operationsMap.get("o4"), Set.of(t2, t3));
        Task t6 = new Task("Task 6", operationsMap.get("o4"), Set.of(t4, t5));

        Set<Task> completePath = Set.of(t1, t2, t3, t4, t5, t6);

        Set<Task> endingTasks = completePath.stream().filter(Task.isTerminalTask).collect(Collectors.toSet());
        Set<Task> startingTasks = completePath.stream().filter(Task.isInitialTask).collect(Collectors.toSet());

        Map<Task,Collection<Flux<?>>> tasksRelationships = new ConcurrentHashMap<>();

        BiConsumer<Task,Flux<?>> injectFluxIntoNextTask = ((next, flux) -> {
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

        System.out.println("tototo");

        tasksRelationships.forEach((key, value) -> {
            Flux<?> flux = key.getWrappedOperation().process(convertCollectionToArray.apply(value));
            flux.log().subscribe(System.out::println);
        });

    }

    Function<Collection<Flux<?>>,Flux<?>[]> convertCollectionToArray = collection -> {
        Flux<?>[] array = new Flux[collection.size()];
        return collection.toArray(array);
    };

}