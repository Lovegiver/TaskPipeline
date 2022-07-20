package com.citizenweb.tooling.taskpipeline.core.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link org.springframework.http.codec.ServerSentEvent} objects need a unique ID.<br>
 * This class exposes a method responsible for incrementally generating such an ID.
 */
public class ServerSentEventCounter {

    private static final AtomicInteger EVENT_ID = new AtomicInteger(1);

    /**
     * Creates a unique ID for {@link org.springframework.http.codec.ServerSentEvent}
     * @return an {@link Integer}
     */
    public static int getEventID() {
        return EVENT_ID.getAndIncrement();
    }
}