package com.level11data.databricks.util;

public class ResourceConfigException extends Exception {
    public ResourceConfigException() {
        super();
    }

    public ResourceConfigException(String message) {
        super(message);
    }

    public ResourceConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceConfigException(Throwable cause) {
        super(cause);
    }
}
