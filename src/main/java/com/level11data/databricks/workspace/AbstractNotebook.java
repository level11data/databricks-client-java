package com.level11data.databricks.workspace;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.WorkspaceClient;
import com.level11data.databricks.client.entities.workspace.DeleteRequestDTO;
import com.level11data.databricks.client.entities.workspace.MkdirsRequestDTO;
import com.level11data.databricks.command.Command;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.util.ResourceUtils;
import com.level11data.databricks.workspace.util.WorkspaceHelper;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public abstract class AbstractNotebook implements Notebook {
    private final WorkspaceClient _client;
    private final String _workspacePath;

    private String _name;
    private WorkspaceHelper _workspaceHelper;

    private final Command[] _commands;

    public AbstractNotebook(WorkspaceClient client, String workspacePath) throws WorkspaceConfigException {
        _client = client;
        _workspacePath = workspacePath;

        ArrayList<Command> commandList = getWorkspaceHelper().parseCommands(workspacePath);
        _commands = commandList.toArray(new Command[commandList.size()]);
    }

    public AbstractNotebook(WorkspaceClient client, String workspacePath, Command[] commands) {
        _client = client;
        _workspacePath = workspacePath;
        _commands = commands;

        //mkdir of parent path of notebook
        String workspacePathWithoutNotebook = Paths.get(_workspacePath).getParent().toString();
        MkdirsRequestDTO mkdirsRequestDTO = new MkdirsRequestDTO();
        mkdirsRequestDTO.Path = workspacePathWithoutNotebook;

        try {
            _client.mkdirs(mkdirsRequestDTO);
        } catch(HttpException e) {
            //swallow RESOURCE_ALREADY_EXISTS
            System.out.println(e);
        }

    }

    private WorkspaceHelper getWorkspaceHelper() {
        if(_workspaceHelper == null) {
            _workspaceHelper = new WorkspaceHelper(_client);
        }
        return _workspaceHelper;
    }

    public String getWorkspacePath() {
        return _workspacePath;
    }

    public void delete() throws WorkspaceConfigException {
        try{
            DeleteRequestDTO deleteRequestDTO = new DeleteRequestDTO();
            deleteRequestDTO.Path = getWorkspacePath();
            _client.delete(deleteRequestDTO);
        } catch(HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public String getName() {
        if(_name == null) {
            _name = Paths.get(getWorkspacePath()).getFileName().toString();
        }
        return _name;
    }

    public Command[] getCommands() {
        return _commands;
    }

    public File saveAsHtml(String pathname) throws WorkspaceConfigException {
        byte [] exportedBytes = getWorkspaceHelper().exportNotebook(getWorkspacePath(), ExportFormat.HTML);

        try {
            return ResourceUtils.writeBytesToFile(exportedBytes, pathname);
        }catch(ResourceConfigException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public File saveAsDbc(String pathname) throws WorkspaceConfigException {
        byte [] exportedBytes = getWorkspaceHelper().exportNotebook(getWorkspacePath(), ExportFormat.DBC);

        try {
            return ResourceUtils.writeBytesToFile(exportedBytes, pathname);
        }catch(ResourceConfigException e) {
            throw new WorkspaceConfigException(e);
        }
    }

}
