package com.level11data.databricks.workspace.builder;

import com.level11data.databricks.workspace.Notebook;
import com.level11data.databricks.workspace.WorkspaceConfigException;

public interface NotebookBuilder {

    NotebookBuilder withScalaCommand(String command);

    NotebookBuilder withPythonCommand(String command);

    NotebookBuilder withRCommand(String command);

    NotebookBuilder withSqlCommand(String command);

    NotebookBuilder withShellCommand(String command);

    NotebookBuilder withDbfsCommand(String command);

    Notebook create(String workspacePath) throws WorkspaceConfigException;

}
