package com.level11data.databricks.workspace;

import com.level11data.databricks.command.Command;

import java.io.File;

public interface Notebook {

    void delete() throws WorkspaceConfigException;

    String getName();

    Command[] getCommands();

    String getWorkspacePath();

    File saveAsDbc(String pathname) throws WorkspaceConfigException;

    File saveAsHtml(String pathname) throws WorkspaceConfigException;

    File saveAsSource(String pathname) throws WorkspaceConfigException;

}