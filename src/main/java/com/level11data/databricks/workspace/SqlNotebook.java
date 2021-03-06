package com.level11data.databricks.workspace;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.WorkspaceClient;
import com.level11data.databricks.client.entities.workspace.ImportRequestDTO;
import com.level11data.databricks.command.Command;
import com.level11data.databricks.command.SqlCommand;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.util.ResourceUtils;

import java.io.File;
import java.io.IOException;


public class SqlNotebook extends AbstractNotebook {

    private WorkspaceClient _client;

    public SqlNotebook(WorkspaceClient workspaceClient, String workspacePath) throws WorkspaceConfigException {
        super(workspaceClient, workspacePath);
        _client = workspaceClient;
    }

    public SqlNotebook(WorkspaceClient workspaceClient, String workspacePath, Command[] commands) throws WorkspaceConfigException {
        super(workspaceClient, workspacePath, commands);
        _client = workspaceClient;

        //build the source code text of the notebook
        StringBuilder sourceCode = getSourceCode();

        try {
            //import notebook
            ImportRequestDTO importRequestDTO = new ImportRequestDTO();
            importRequestDTO.Path = getWorkspacePath();
            importRequestDTO.Format = ExportFormat.SOURCE.toString();
            importRequestDTO.Language = NotebookLanguage.SQL.toString();
            importRequestDTO.Overwrite = true;
            importRequestDTO.Content = ResourceUtils.encodeToBase64(sourceCode.toString().getBytes());
            _client.importResource(importRequestDTO);
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        } catch(HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    private StringBuilder getSourceCode() {
        int index = 0;

        //build the source code text of the notebook
        StringBuilder sourceCode = new StringBuilder();
        sourceCode.append("-- Databricks notebook source");
        sourceCode.append(System.lineSeparator());
        for (Command command : getCommands()) {
            if(command instanceof SqlCommand) {
                sourceCode.append(command.getCommand());
                sourceCode.append(System.lineSeparator());
            } else {
                sourceCode.append("-- MAGIC ");
                sourceCode.append(command.getCommandWithDirective());
                sourceCode.append(System.lineSeparator());
            }

            if(index < getCommands().length) {
                sourceCode.append("-- COMMAND ----------");
                sourceCode.append(System.lineSeparator());
            }
            index++;
        }
        return sourceCode;
    }

    public File saveAsSource(String pathname) throws WorkspaceConfigException {
        //build the source code text of the notebook
        StringBuilder sourceCode = getSourceCode();

        //write text to file
        try {
            return ResourceUtils.writeTextFile(sourceCode, pathname);
        } catch(ResourceConfigException e) {
            throw new WorkspaceConfigException(e);
        }
    }

}
