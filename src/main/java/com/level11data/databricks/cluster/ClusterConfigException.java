package com.level11data.databricks.cluster;

public class ClusterConfigException extends Exception {
    public ClusterConfigException() {
        super();
    }

    public ClusterConfigException(String message) {
        super(message);
    }

    public ClusterConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterConfigException(Throwable cause) {
        super(cause);
    }

}
