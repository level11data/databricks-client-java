package com.level11data.databricks.job.run;

public class JobRunException extends Exception {
    public JobRunException() {
        super();
    }

    public JobRunException(String message) {
        super(message);
    }

    public JobRunException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobRunException(Throwable cause) {
        super(cause);
    }
}
