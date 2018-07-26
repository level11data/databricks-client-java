package com.level11data.databricks.workspace;

public class WorkspaceConfigException extends Exception {
    public WorkspaceConfigException() {
        super();
    }

    public WorkspaceConfigException(String message) {
        super(message);
    }

    public WorkspaceConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkspaceConfigException(Throwable cause) {
        super(cause);
    }
}
