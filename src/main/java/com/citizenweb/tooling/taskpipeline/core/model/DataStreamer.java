package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.MonitorDTO;
import com.citizenweb.tooling.taskpipeline.core.utils.ServerSentEventCounter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The {@link DataStreamer} is in charge of exporting the {@link Pipeline}'s current state for display purpose.<br>
 * It is a Singleton.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataStreamer {

    private final ConcurrentHashMap<Pipeline, List<ServerSentEvent<String>>> eventQueue = new ConcurrentHashMap<>();

    private static final DataStreamer DATA_STREAMER = new DataStreamer();

    private final Function<Pipeline, MonitorDTO> convertToDTO = MonitorDTO::new;

    private final Function<MonitorDTO,ServerSentEvent<String>> convertToSSE = dto -> {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json;
        try {
            json = ow.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return ServerSentEvent.<String>builder()
                .id(String.valueOf(ServerSentEventCounter.getEventID()))
                .event("Chrono")
                .retry(Duration.of(1, ChronoUnit.SECONDS))
                .data(json)
                .build();
    };

    public static DataStreamer getInstance() {
        return DATA_STREAMER;
    }

    public Flux<ServerSentEvent<String>> exportData() {
        log.info("In Exporter -> Queue size = " + eventQueue.size());
        return Flux.create(sse -> eventQueue.values().stream().flatMap(Collection::stream).forEach(sse::next));
    }

    public Flux<ServerSentEvent<String>> exportData(Pipeline pipeline) {
        log.info("In Exporter -> Queue size = " + eventQueue.size());
        return Flux.create(sse -> eventQueue.entrySet().stream()
                .filter(entry -> pipeline == entry.getKey())
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .forEach(sse::next));
    }

    public void triggerNotification(Pipeline pipeline) {
        log.info("In Consumer -> Pipeline = " + pipeline);
        List<ServerSentEvent<String>> serverSentEvents;
        if (eventQueue.get(pipeline) == null) {
            serverSentEvents = new ArrayList<>();
        } else {
            serverSentEvents = eventQueue.get(pipeline);
        }
        serverSentEvents.add(convertToDTO.andThen(convertToSSE).apply(pipeline));
        this.eventQueue.put(pipeline, serverSentEvents);
    }

}
