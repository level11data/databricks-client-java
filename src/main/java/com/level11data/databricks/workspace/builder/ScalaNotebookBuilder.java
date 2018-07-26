package com.level11data.databricks.workspace.builder;

import com.level11data.databricks.client.WorkspaceClient;
import com.level11data.databricks.workspace.Notebook;
import com.level11data.databricks.workspace.ScalaNotebook;
import com.level11data.databricks.workspace.WorkspaceConfigException;

import java.io.File;

public class ScalaNotebookBuilder extends AbstractNotebookBuilder {

    private final WorkspaceClient _client;

    public ScalaNotebookBuilder(WorkspaceClient client, Notebook notebook) throws WorkspaceConfigException {
        super(client, notebook);
        _client = client;
    }

    public ScalaNotebookBuilder(WorkspaceClient client, File file) throws WorkspaceConfigException {
        super(client, file);
        _client = client;
    }

    public ScalaNotebookBuilder(WorkspaceClient client) throws WorkspaceConfigException {
        super(client);
        _client = client;
    }

    @Override
    public ScalaNotebookBuilder withScalaCommand(String command) {
        return (ScalaNotebookBuilder)super.withScalaCommand(command);
    }

    @Override
    public ScalaNotebookBuilder withPythonCommand(String command) {
        return (ScalaNotebookBuilder)super.withPythonCommand(command);
    }

    @Override
    public ScalaNotebookBuilder withRCommand(String command) {
        return (ScalaNotebookBuilder)super.withRCommand(command);
    }

    @Override
    public ScalaNotebookBuilder withSqlCommand(String command) {
        return (ScalaNotebookBuilder)super.withScalaCommand(command);
    }

    @Override
    public ScalaNotebookBuilder withShellCommand(String command) {
        return (ScalaNotebookBuilder)super.withShellCommand(command);
    }

    @Override
    public ScalaNotebookBuilder withDbfsCommand(String command) {
        return (ScalaNotebookBuilder)super.withDbfsCommand(command);
    }

    public ScalaNotebook create(String workspacePath) throws WorkspaceConfigException {
        return new ScalaNotebook(_client, workspacePath, getCommands());
    }
}
