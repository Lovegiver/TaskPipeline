package com.citizenweb.tooling.taskpipeline.core.model;

import lombok.Getter;

public class StateNotifier implements Notifier {


    private final Pipeline parent;

    @Getter
    private final DataStreamer streamer = DataStreamer.getInstance();

    public StateNotifier(Pipeline parent) {
        this.parent = parent;
    }

    @Override
    public void notifyStateChange() {
        this.streamer.triggerNotification(this.parent);
    }

}
