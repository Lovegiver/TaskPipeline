package data;

import com.citizenweb.tooling.taskpipeline.core.model.Operation;
import lombok.Getter;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class DataForTests {

    @Getter
    private static final Map<String, Operation> data = new HashMap<>();

    public static void initData() {
        if (data.isEmpty()) {
            Operation o1 = inputs -> Flux.interval(Duration.ofSeconds(1)).range(1,10);
            Operation o2 = inputs -> Flux.range(91,10);
            Operation o3 = inputs -> Flux.just(18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
            Operation o4 = inputs -> {
                Flux<?> int1 = inputs[0];
                Flux<?> int2 = inputs[1];
                return Flux.interval(Duration.ofSeconds(1)).zip(int1, int2, (x, y) -> (int) x + (int) y);
            };
            @SuppressWarnings("SpellCheckingInspection")
            Operation o5 = inputs -> Flux.just("pangolins", "chien",
                    "chat", "tomates", "hamburgers", "voitures", "maisons", "personnes", "percolateurs", "souris");
            Operation o6 = inputs -> {
                Flux<?> int1 = inputs[0];
                Flux<?> word1 = inputs[1];
                return Flux.interval(Duration.ofSeconds(1)).zip(int1, word1, (i, w) -> String.format("I've just seen %d %s flying ^^", (int) i, w));
            };
            Operation o7 = inputs -> {
                Flux<?> int1 = inputs[0];
                Flux<?> word1 = inputs[1];
                return Flux.interval(Duration.ofSeconds(1)).zip(int1, word1, (i, w) -> String.format("I've said [ %s ] around [ %d ] times !!", w, (int) i));
            };
            data.put("Count to 10", o1);
            data.put("Count to 100", o2);
            data.put("Reverse count to 0", o3);
            data.put("Sum", o4);
            data.put("Send word", o5);
            data.put("Create sentence", o6);
            data.put("Top task", o7);
        }
    }

}
