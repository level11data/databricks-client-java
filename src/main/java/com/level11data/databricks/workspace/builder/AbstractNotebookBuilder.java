package com.level11data.databricks.workspace.builder;

import com.level11data.databricks.client.WorkspaceClient;
import com.level11data.databricks.workspace.*;
import com.level11data.databricks.command.*;
import com.level11data.databricks.workspace.util.WorkspaceHelper;

import java.io.*;
import java.util.ArrayList;

public abstract class AbstractNotebookBuilder implements NotebookBuilder {

    private final WorkspaceClient _client;
    private final ArrayList<Command> _commands;

    public AbstractNotebookBuilder(WorkspaceClient client, Notebook notebook) throws WorkspaceConfigException {
        _client = client;
        _commands = new ArrayList<>();

        for (Command command : notebook.getCommands()) {
            _commands.add(command);
        }
    }

    public AbstractNotebookBuilder(WorkspaceClient client, File file) throws WorkspaceConfigException {
        _client = client;

        WorkspaceHelper helper = new WorkspaceHelper(_client);
        _commands = helper.parseCommands(file);
    }

    public AbstractNotebookBuilder(WorkspaceClient client) throws WorkspaceConfigException {
        _client = client;
        _commands = new ArrayList<>();
    }

    public AbstractNotebookBuilder withScalaCommand(String command) {
        this._commands.add(new ScalaCommand(command));
        return this;
    }

    public AbstractNotebookBuilder withPythonCommand(String command) {
        this._commands.add(new PythonCommand(command));
        return this;
    }

    public AbstractNotebookBuilder withRCommand(String command) {
        this._commands.add(new RCommand(command));
        return this;
    }

    public AbstractNotebookBuilder withSqlCommand(String command) {
        this._commands.add(new SqlCommand(command));
        return this;
    }

    public AbstractNotebookBuilder withShellCommand(String command) {
        this._commands.add(new ShellCommand(command));
        return this;
    }

    public AbstractNotebookBuilder withDbfsCommand(String command) {
        this._commands.add(new DbfsCommand(command));
        return this;
    }

    public AbstractNotebookBuilder withEmptyCommand() {
        this._commands.add(new EmptyCommand());
        return this;
    }

    public Command[] getCommands() {
        return _commands.toArray(new Command[_commands.size()]);
    }
}
