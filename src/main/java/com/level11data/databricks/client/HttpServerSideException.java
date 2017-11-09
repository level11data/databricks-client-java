package com.level11data.databricks.client;


public class HttpServerSideException extends HttpException {
    public HttpServerSideException() {
        super();
    }

    public HttpServerSideException(String message) {
        super(message);
    }

    public HttpServerSideException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpServerSideException(Throwable cause) {
        super(cause);
    }
}
