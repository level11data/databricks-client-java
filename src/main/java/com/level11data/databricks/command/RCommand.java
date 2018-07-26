package com.level11data.databricks.command;

public class RCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%r";

    public RCommand(String command) {
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }
}
