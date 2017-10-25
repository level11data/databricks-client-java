package com.level11data.databricks.job;

public class JobConfigException extends Exception {
    public JobConfigException() {
        super();
    }

    public JobConfigException(String message) {
        super(message);
    }

    public JobConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobConfigException(Throwable cause) {
        super(cause);
    }
}
