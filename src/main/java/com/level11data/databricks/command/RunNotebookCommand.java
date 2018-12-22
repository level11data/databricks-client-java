package com.level11data.databricks.command;

public class RunNotebookCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%run";

    public RunNotebookCommand() {
        super("");
    }

    public String getCommandWithDirective() {
        return "";
    }
}
