package com.citizenweb.tooling.taskpipeline.core.utils;

/**
 * States for a {@link com.citizenweb.tooling.taskpipeline.core.model.Monitorable} object
 */
public enum ProcessingStatus {
    NEW,
    RUNNING,
    IN_ERROR,
    DONE
}
