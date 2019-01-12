package com.level11data.databricks.workspace.util;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.WorkspaceClient;
import com.level11data.databricks.client.entities.workspace.*;
import com.level11data.databricks.command.*;
import com.level11data.databricks.util.ResourceUtils;
import com.level11data.databricks.workspace.*;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.ArrayList;

public class WorkspaceHelper {

    private final WorkspaceClient _client;

    public WorkspaceHelper(WorkspaceClient client) {
        _client = client;
    }

    public ObjectStatus getStatus(String workspacePath) throws WorkspaceConfigException {
        try {
            StatusRequestDTO statusRequestDTO = new StatusRequestDTO();
            statusRequestDTO.Path = workspacePath;
            return new ObjectStatus(_client.getStatus(statusRequestDTO));
        } catch(HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public Notebook getNotebook(String workspacePath) throws WorkspaceConfigException {
        ObjectStatus resourceStatus = getStatus(workspacePath);

        if(resourceStatus.Type != ObjectType.NOTEBOOK) {
            throw new WorkspaceConfigException("Path given does not represent Notebook");
        }

        if(resourceStatus.NotebookLanguage == NotebookLanguage.SCALA) {
            return new ScalaNotebook(_client, workspacePath);
        } else {
            throw new WorkspaceConfigException("Unsupported Notebook Language: " + resourceStatus.NotebookLanguage);
        }
    }

    public ArrayList<Command> parseCommands(File file) throws WorkspaceConfigException {
        //get file extension
        String fileExtension;
        try {
            fileExtension = FilenameUtils.getExtension(file.getCanonicalPath());
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        }

        if(fileExtension.equalsIgnoreCase("html") || fileExtension.equalsIgnoreCase("dbc")) {
            //file is a non-source type
            RemoteNotebook remoteNotebook = decodeNonSourceFile(file);
            return parseCommands(remoteNotebook);
        } else {
            //file must be source code
            CommandLanguage fileLang = getSourceLang(file);

            try(FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                return parseCommands(bufferedReader, fileLang);
            } catch(IOException e) {
                throw new WorkspaceConfigException(e);
            }
        }
    }

    public ArrayList<Command> parseCommands(String workspacePath) throws WorkspaceConfigException {
        //export notebook to source
        byte[] notebookBytes = exportNotebook(workspacePath, ExportFormat.SOURCE);

        //determine notebook language
        NotebookLanguage notebookLanguage = getStatus(workspacePath).NotebookLanguage;

        //combine bytes with notebook language
        RemoteNotebook remoteNotebook = new RemoteNotebook(notebookLanguage, notebookBytes);


        return parseCommands(remoteNotebook);
    }

    private ArrayList<Command> parseCommands(RemoteNotebook remoteNotebook) throws WorkspaceConfigException {
        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(remoteNotebook.SourceCode);
        InputStreamReader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader)) {
            return parseCommands(bufferedReader, getCommandLanguage(remoteNotebook.Language));
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    private ArrayList<Command> parseCommands(BufferedReader bufferedReader, CommandLanguage defaultCommandLang) throws WorkspaceConfigException {
        ArrayList<Command> commands = new ArrayList<>();

        try {
            String line = bufferedReader.readLine();
            StringBuilder commandCell = new StringBuilder();

            CommandLanguage commandLang = defaultCommandLang;

            while (line != null) {
                if(commandCell.length() > 0) {
                    //add new line if this is
                    commandCell.append(System.lineSeparator());
                }

                //check if line begins with directive
                if(line.contains("Databricks notebook source")) {
                    //typical first line for notebook exported from Databricks
                    //no-opp - skip
                } else if(line.startsWith("%scala")) {
                    commandLang = CommandLanguage.SCALA;
                    commandCell = parseDirectiveLine(commandCell, line, "%scala");
                } else if(line.startsWith("%python")) {
                    commandLang = CommandLanguage.PYTHON;
                    commandCell = parseDirectiveLine(commandCell, line, "%python");
                } else if(line.startsWith("%py")) {
                    commandLang = CommandLanguage.PYTHON;
                    commandCell = parseDirectiveLine(commandCell, line, "%py");
                } else if(line.startsWith("%sql")) {
                    commandLang = CommandLanguage.SQL;
                    commandCell = parseDirectiveLine(commandCell, line, "%sql");
                } else if(line.startsWith("%r")) {
                    commandLang = CommandLanguage.R;
                    commandCell = parseDirectiveLine(commandCell, line, "%r");
                } else if(line.startsWith("%sh")) {
                    commandLang = CommandLanguage.SHELL;
                    commandCell = parseDirectiveLine(commandCell, line, "%sh");
                } else if(line.startsWith("%fs")) {
                    commandLang = CommandLanguage.DBFS;
                    commandCell = parseDirectiveLine(commandCell, line, "%fs");
                }  else if(line.startsWith("%md")) {
                    commandLang = CommandLanguage.MARKDOWN;
                    commandCell = parseDirectiveLine(commandCell, line, "%md");
                }else if (line.contains("COMMAND ----------")) {
                    //add command to list
                    commands.add(createCommand(commandLang, commandCell.toString()));

                    //start new command
                    commandCell = new StringBuilder();
                    commandLang = defaultCommandLang;
                } else {
                    //command is in same language as notebook base language
                    commandCell.append(line);
                }
                //read next line
                line = bufferedReader.readLine();
            }
            //add final command
            if(commandCell.length() > 0) {
                commands.add(createCommand(commandLang, commandCell.toString()));
            }
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        }
        return commands;
    }

    private CommandLanguage getSourceLang(File file) throws WorkspaceConfigException {
        try {
            //programing language of file
            String fileExtension = FilenameUtils.getExtension(file.getCanonicalPath());

            if(fileExtension.equalsIgnoreCase("scala")) {
                return CommandLanguage.SCALA;
            } else if(fileExtension.equalsIgnoreCase("python")) {
                return CommandLanguage.PYTHON;
            } else if(fileExtension.equalsIgnoreCase("py")) {
                return CommandLanguage.PYTHON;
            } else if(fileExtension.equalsIgnoreCase("r")) {
                return CommandLanguage.R;
            } else if(fileExtension.equalsIgnoreCase("sql")) {
                return CommandLanguage.SQL;
            } else if(fileExtension.equalsIgnoreCase("ipynb")) {
                return CommandLanguage.PYTHON;
            } else {
                throw new WorkspaceConfigException("Unsupported Source Code Filename Extension: " + fileExtension);
            }
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    private CommandLanguage getCommandLanguage(NotebookLanguage notebookLanguage) throws WorkspaceConfigException {
        if(notebookLanguage.equals(NotebookLanguage.PYTHON)) {
            return CommandLanguage.PYTHON;
        } else if(notebookLanguage.equals(NotebookLanguage.SCALA)) {
            return CommandLanguage.SCALA;
        } else if(notebookLanguage.equals(NotebookLanguage.R)) {
            return CommandLanguage.R;
        } else if(notebookLanguage.equals(NotebookLanguage.SQL)) {
            return CommandLanguage.SQL;
        } else {
            throw new WorkspaceConfigException("Unsupported Notebook Language: " + notebookLanguage.toString());
        }
    }

    public byte[] exportNotebook(String workspacePath, ExportFormat exportFormat) throws WorkspaceConfigException {
        ExportRequestDTO exportRequestDTO = new ExportRequestDTO();
        exportRequestDTO.Path = workspacePath;
        exportRequestDTO.Format = exportFormat.toString();
        byte[] notebookBytes;
        try {
            ExportResponseDTO exportResponseDTO = _client.exportResource(exportRequestDTO);
            return ResourceUtils.decodeFromBase64(exportResponseDTO.Content);
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        } catch(HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    private String importNotebook(File file) throws WorkspaceConfigException {
        String tmpWorkspacePath = "/tmp/"+ System.currentTimeMillis();
        String tmpWorkspacePathWithNotebook = tmpWorkspacePath + "/" + file.getName();

        try {
            WorkspaceMkdirsRequestDTO workspaceMkdirsRequestDTO = new WorkspaceMkdirsRequestDTO();
            workspaceMkdirsRequestDTO.Path = tmpWorkspacePath;
            _client.mkdirs(workspaceMkdirsRequestDTO);

            ImportRequestDTO importRequestDTO = new ImportRequestDTO();
            importRequestDTO.Format = getExportFormat(file).toString();
            importRequestDTO.Path = tmpWorkspacePathWithNotebook;

            importRequestDTO.Content = ResourceUtils.encodeToBase64(file);
            _client.importResource(importRequestDTO);
            return tmpWorkspacePathWithNotebook;
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        } catch(HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    private ExportFormat getExportFormat(File file) throws WorkspaceConfigException {
        String fileExtension;

        //get file extension
        try {
            fileExtension = FilenameUtils.getExtension(file.getCanonicalPath());
        } catch(IOException e) {
            throw new WorkspaceConfigException(e);
        }

        //check if file is a supported (non-source) type
        if(fileExtension.equalsIgnoreCase("html")) {
            return ExportFormat.HTML;
        } else if(fileExtension.equalsIgnoreCase("dbc")) {
            return ExportFormat.DBC;
        } else if(fileExtension.equalsIgnoreCase("scala")) {
            return ExportFormat.SOURCE;
        } else if(fileExtension.equalsIgnoreCase("python")) {
            return ExportFormat.SOURCE;
        } else if(fileExtension.equalsIgnoreCase("py")) {
            return ExportFormat.SOURCE;
        } else if(fileExtension.equalsIgnoreCase("r")) {
            return ExportFormat.SOURCE;
        } else if(fileExtension.equalsIgnoreCase("sql")) {
            return ExportFormat.SOURCE;
        } else if(fileExtension.equalsIgnoreCase("ipynb")) {
            return ExportFormat.JUPYTER;
        } else if(fileExtension.equalsIgnoreCase("Rmd")) {
            throw new WorkspaceConfigException("R Markdown Export not supported yet FEATURE-2889");
        } else {
            throw new WorkspaceConfigException("Unsupported non-source file extension: "+ fileExtension);
        }
    }

    private void validateNonSourceFile(File file) throws WorkspaceConfigException {
        ExportFormat exportFormat = getExportFormat(file);

        //TODO Add RMarkdown Export Format

        //check if file is a supported (non-source) type
        if(exportFormat.equals(ExportFormat.DBC)) {
            //no op all good
        } else if(exportFormat.equals(ExportFormat.HTML)) {
            //no op all good
        } else if(exportFormat.equals(ExportFormat.JUPYTER)) {
                //no op all good
        } else {
            //get file extension
            try {
                String fileExtension = FilenameUtils.getExtension(file.getCanonicalPath());
                throw new WorkspaceConfigException("Unsupported non-source file extension: "+ fileExtension);
            } catch(IOException e) {
                throw new WorkspaceConfigException(e);
            }
        }

    }

    private RemoteNotebook decodeNonSourceFile(File file) throws WorkspaceConfigException {
        //check if file is a supported (non-source) type
        validateNonSourceFile(file);

        //import dbc/html file as a temp notebook
        String tmpWorkspacePath = importNotebook(file);

        //export notebook to source
        byte[] notebookBytes = exportNotebook(tmpWorkspacePath, ExportFormat.SOURCE);

        //determine notebook language
        NotebookLanguage notebookLanguage = getStatus(tmpWorkspacePath).NotebookLanguage;

        //delete temp notebook
        _client.Session.deleteWorkspaceObject(tmpWorkspacePath, false);

        return new RemoteNotebook(notebookLanguage, notebookBytes);
    }

    private Command createCommand(CommandLanguage lang, String command) throws WorkspaceConfigException {
        if(lang.equals(CommandLanguage.SCALA)) {
            return new ScalaCommand(command);
        } else if(lang.equals(CommandLanguage.PYTHON)) {
            return new PythonCommand(command);
        } else if(lang.equals(CommandLanguage.R)) {
            return new RCommand(command);
        } else if(lang.equals(CommandLanguage.SQL)) {
            return new SqlCommand(command);
        } else if(lang.equals(CommandLanguage.SHELL)) {
            return new ShellCommand(command);
        } else if(lang.equals(CommandLanguage.DBFS)) {
            return new DbfsCommand(command);
        } else {
            throw new WorkspaceConfigException("Unsuppoted Command Language: " + lang.toString());
        }
    }

    private StringBuilder parseDirectiveLine(StringBuilder command, String line, String directive) {
        //check for directive
        if(line.startsWith(directive)) {
            //strip directive and append rest of line (if any)
            if(line.split(directive).length > 0) {
                command.append(line.split(directive)[1]);
            }
        }
        return command;
    }

}
