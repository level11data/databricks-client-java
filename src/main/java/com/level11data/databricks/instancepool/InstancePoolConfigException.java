package com.level11data.databricks.instancepool;

public class InstancePoolConfigException extends Exception {
    public InstancePoolConfigException() {
        super();
    }

    public InstancePoolConfigException(String message) {
        super(message);
    }

    public InstancePoolConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public InstancePoolConfigException(Throwable cause) {
        super(cause);
    }
}
