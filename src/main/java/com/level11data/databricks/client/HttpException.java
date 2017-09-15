package com.level11data.databricks.client;

public class HttpException extends Exception {
    public HttpException() {
        super();
    }

    public HttpException(String message) {
      super(message);
    }

    public HttpException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpException(Throwable cause) {
        super(cause);
    }
}
