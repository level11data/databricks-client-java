package com.level11data.databricks.client;

public class DatabricksConfigException extends Exception {

    public DatabricksConfigException() {
        super();
    }

    public DatabricksConfigException(String message) {
        super(message);
    }

    public DatabricksConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabricksConfigException(Throwable cause) {
        super(cause);
    }
}
