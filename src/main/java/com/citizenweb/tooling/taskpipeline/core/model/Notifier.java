package com.citizenweb.tooling.taskpipeline.core.model;

@FunctionalInterface
public interface Notifier {

    void notifyStateChange();

}
