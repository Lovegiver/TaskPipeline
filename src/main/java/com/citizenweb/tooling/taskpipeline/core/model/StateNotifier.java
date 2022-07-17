package com.citizenweb.tooling.taskpipeline.core.model;

import lombok.Getter;

/**
 * The {@link StateNotifier} contains a reference to the {@link Pipeline}. Thus; when its {@link #notifyStateChange()}
 * will be triggered, it will send the {@link Pipeline}'s current state to the {@link DataStreamer}.
 */
public class StateNotifier implements Notifier {

    /** The {@link Pipeline} to notify about */
    private final Pipeline parent;
    /** The {@link DataStreamer} that will effectively export the {@link Pipeline}'s state */
    @Getter
    private final DataStreamer streamer = DataStreamer.getInstance();

    public StateNotifier(Pipeline parent) {
        this.parent = parent;
    }

    /**
     * Sends the {@link Pipeline} to the {@link DataStreamer}
     */
    @Override
    public void notifyStateChange() {
        this.streamer.triggerNotification(this.parent);
    }

}
