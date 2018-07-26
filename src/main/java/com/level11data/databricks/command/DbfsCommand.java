package com.level11data.databricks.command;

public class DbfsCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%fs";

    public DbfsCommand(String command) {
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }
}
