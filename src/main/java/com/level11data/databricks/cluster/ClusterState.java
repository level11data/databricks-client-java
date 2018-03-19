package com.level11data.databricks.cluster;

public enum ClusterState {
    PENDING, RUNNING, RESTARTING, RESIZING, TERMINATING, TERMINATED, ERROR, UNKNOWN;

    public boolean isFinal() {
        if(this.equals(ClusterState.TERMINATED)) return true;
        if(this.equals(ClusterState.ERROR)) return true;
        if(this.equals(ClusterState.UNKNOWN)) return true;

        //otherwise the cluster is doing something
        return false;
    }
}

