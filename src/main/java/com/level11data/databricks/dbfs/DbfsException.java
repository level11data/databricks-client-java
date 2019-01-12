package com.level11data.databricks.dbfs;

public class DbfsException extends Exception {
    public DbfsException() {
        super();
    }

    public DbfsException(String message) {
        super(message);
    }

    public DbfsException(String message, Throwable cause) {
        super(message, cause);
    }

    public DbfsException(Throwable cause) {
        super(cause);
    }
}
