package com.level11data.databricks.command;

public class SqlCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%sql";

    public SqlCommand(String command){
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }
}
