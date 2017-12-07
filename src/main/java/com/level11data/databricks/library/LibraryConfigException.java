package com.level11data.databricks.library;

public class LibraryConfigException extends Exception {
    public LibraryConfigException() {
        super();
    }

    public LibraryConfigException(String message) {
        super(message);
    }

    public LibraryConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public LibraryConfigException(Throwable cause) {
        super(cause);
    }
}
