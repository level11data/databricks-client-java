package com.level11data.databricks.command;

public class EmptyCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "";

    public EmptyCommand() {
        super("");
    }

    public String getCommandWithDirective() {
        return "";
    }
}
