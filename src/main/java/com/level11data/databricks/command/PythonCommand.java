package com.level11data.databricks.command;

public class PythonCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%python";

    public PythonCommand(String command) {
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }
}
