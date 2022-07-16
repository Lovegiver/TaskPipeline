package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.utils.ProcessingStatus;
import com.citizenweb.tooling.taskpipeline.core.utils.ProcessingType;
import lombok.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Object in charge of life cycle management
 */
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Monitor {
    @Getter
    @EqualsAndHashCode.Include
    private final String id;
    @Getter
    private final ProcessingType type;
    @Getter @Setter
    private ProcessingStatus status;
    @Getter @Setter
    private LocalDateTime startTime;
    @Getter @Setter
    private LocalDateTime endTime;
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

    public void statusToRunning() {
        this.status = ProcessingStatus.RUNNING;
        this.startTime = LocalDateTime.now();
    }

    public void statusToDone() {
        this.status = ProcessingStatus.DONE;
        this.endTime = LocalDateTime.now();
        this.duration = Duration.between(startTime, endTime).toMillis();
    }

    public void statusToError() {
        this.status = ProcessingStatus.IN_ERROR;
        this.endTime = LocalDateTime.now();
        this.duration = Duration.between(startTime, endTime).toMillis();
    }

}
