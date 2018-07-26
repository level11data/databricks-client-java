package com.level11data.databricks.command;

public abstract class AbstractCommand implements Command {

    private final String _command;

    public AbstractCommand(String command) {
        _command = command;
    }

    public String getCommand() {
        return _command;
    }

    protected String getCommandWithDirective(String directive) {
        StringBuilder builder = new StringBuilder();
        builder.append(directive);
        builder.append(System.lineSeparator());
        builder.append(getCommand());
        return builder.toString();
    }

}
