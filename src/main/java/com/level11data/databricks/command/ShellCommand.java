package com.level11data.databricks.command;

public class ShellCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%sh";

    public ShellCommand(String command) {
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }
}
