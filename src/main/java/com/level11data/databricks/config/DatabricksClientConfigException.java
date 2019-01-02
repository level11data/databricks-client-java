package com.level11data.databricks.config;

public class DatabricksClientConfigException extends Exception {

    public DatabricksClientConfigException() {
        super();
    }

    public DatabricksClientConfigException(String message) {
        super(message);
    }

    public DatabricksClientConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabricksClientConfigException(Throwable cause) {
        super(cause);
    }
}
