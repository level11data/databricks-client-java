package com.level11data.databricks.job;

public enum RunLifeCycleState {
    PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR;

    public boolean isFinal() {
        if (this == TERMINATED) {
            return true;
        } else if(this == SKIPPED) {
            return true;
        } else if(this == INTERNAL_ERROR) {
            return true;
        } else {
            return false;
        }
    }

}
