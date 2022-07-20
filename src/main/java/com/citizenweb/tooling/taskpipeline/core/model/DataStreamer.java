package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.PipelineDTO;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The {@link DataStreamer} is in charge of exporting the {@link Pipeline}'s current state for display purpose.<br>
 * It is a Singleton.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataStreamer {

    /** A {@link ServerSentEvent} has to be named */
    private final String SSE_EVENT_NAME = "Pipeline";
    /** For each {@link Pipeline} we store a collection of {@link ServerSentEvent}s */
    private final ConcurrentHashMap<Pipeline, CopyOnWriteArrayList<ServerSentEvent<String>>> notificationsMap = new ConcurrentHashMap<>();
    /** Stores the unique instance of {@link DataStreamer} */
    private static final AtomicReference<DataStreamer> DATA_STREAMER = new AtomicReference<>(new DataStreamer());
    /** Converts a {@link Pipeline} into a {@link PipelineDTO} */
    private final Function<Pipeline, PipelineDTO> convertToDTO = PipelineDTO::new;
    /** Converts a {@link PipelineDTO} into a {@link ServerSentEvent} */
    private final Function<PipelineDTO,ServerSentEvent<String>> convertToSSE = dto -> {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json;
        try {
            json = ow.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return ServerSentEvent.<String>builder()
                .id(String.valueOf(ServerSentEventCounter.getEventID()))
                .event(SSE_EVENT_NAME)
                .retry(Duration.of(1, ChronoUnit.SECONDS))
                .data(json)
                .build();
    };

    /** Returns the sole instance of {@link DataStreamer} */
    public static DataStreamer getInstance() {
        return DATA_STREAMER.get();
    }

    /** Creates a {@link Flux} based on the Map's content */
    public synchronized Flux<ServerSentEvent<String>> exportData() {
        log.info("In Exporter -> Queue size = " + notificationsMap.size());
        return Flux.create(sse -> notificationsMap.values().stream()
                .flatMap(Collection::stream)
                .forEach(sse::next));
    }

    /** Creates a {@link Flux} based on Map's content for a given {@link Pipeline} */
    public synchronized Flux<ServerSentEvent<String>> exportData(Pipeline pipeline) {
        log.info("In Exporter -> Queue size = " + notificationsMap.size());
        return Flux.create(sse -> notificationsMap.entrySet().stream()
                .filter(entry -> pipeline == entry.getKey())
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .forEach(sse::next));
    }

    /** Calling this method will snapshot the state of the given {@link Pipeline} */
    public synchronized void triggerNotification(Pipeline pipeline) {
        log.info("In Consumer -> Pipeline = " + pipeline);
        CopyOnWriteArrayList<ServerSentEvent<String>> serverSentEvents;
        if (notificationsMap.get(pipeline) == null) {
            serverSentEvents = new CopyOnWriteArrayList<>();
        } else {
            serverSentEvents = notificationsMap.get(pipeline);
        }
        serverSentEvents.add(convertToDTO.andThen(convertToSSE).apply(pipeline));
        this.notificationsMap.put(pipeline, serverSentEvents);
    }

}
