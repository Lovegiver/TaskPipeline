package com.citizenweb.tooling.taskpipeline.core.model;

/**
 * A {@link Notifier} has a single method  used by any {@link Monitorable} object to trigger the wrapping
 * {@link Pipeline}'s state export.
 * Its usage is a simple implementation of the <b>Visitor pattern</b>
 */
@FunctionalInterface
public interface Notifier {

    /**
     * Notifies the {@link DataStreamer} with the new {@link Pipeline}'s state in order to export it.
     */
    void notifyStateChange();

}
