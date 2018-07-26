package com.level11data.databricks.command;

public class ScalaCommand extends AbstractCommand {
    private String COMMAND_DIRECTIVE = "%scala";

    public ScalaCommand(String command) {
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }
}
