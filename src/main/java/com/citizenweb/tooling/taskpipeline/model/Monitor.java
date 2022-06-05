package com.citizenweb.tooling.taskpipeline.model;

import com.citizenweb.tooling.taskpipeline.utils.ProcessingStatus;
import com.citizenweb.tooling.taskpipeline.utils.ProcessingType;
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
    private Duration duration;

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
        this.duration = Duration.between(startTime, endTime);
    }

    public void statusToError() {
        this.status = ProcessingStatus.IN_ERROR;
        this.endTime = LocalDateTime.now();
        this.duration = Duration.between(startTime, endTime);
    }

}
