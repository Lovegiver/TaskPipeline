package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.ProcessingStatus;
import com.citizenweb.tooling.taskpipeline.core.utils.ProcessingType;
import lombok.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Object in charge of life cycle management for any {@link Monitorable} object.<br>
 * It contains all the data that will be exported to the Front End for display purpose.
 */
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Monitor {
    /** Unique ID based on UUID */
    @Getter
    @EqualsAndHashCode.Include
    private final String id;
    /** Type of the monitored object */
    @Getter
    private final ProcessingType type;
    /** Current status of the monitored object */
    @Getter @Setter
    private ProcessingStatus status;
    /** Starting time */
    @Getter @Setter
    private LocalDateTime startTime;
    /** Ending time */
    @Getter @Setter
    private LocalDateTime endTime;
    /** Computed duration : Ending time - Starting time in Millis */
    @Getter @Setter
    private long duration;
    /** Locates a task within a work path. Terminal task is rank 1 by default. Other tasks ranks depend
     * on their respective location compared to the terminal task */
    @Getter @Setter
    private int rank;

    public Monitor(ProcessingType monitoredObject) {
        this.type = monitoredObject;
        this.status = ProcessingStatus.NEW;
        this.id = UUID.randomUUID().toString();
    }

    /**
     * Set Status to RUNNING
     */
    public void statusToRunning() {
        this.status = ProcessingStatus.RUNNING;
        this.startTime = LocalDateTime.now();
    }

    /**
     * Set Status to DONE
     */
    public void statusToDone() {
        this.status = ProcessingStatus.DONE;
        this.endTime = LocalDateTime.now();
        this.duration = Duration.between(startTime, endTime).toMillis();
    }

    /**
     * Set Status to ERROR
     */
    public void statusToError() {
        this.status = ProcessingStatus.IN_ERROR;
        this.endTime = LocalDateTime.now();
        this.duration = Duration.between(startTime, endTime).toMillis();
    }

}
