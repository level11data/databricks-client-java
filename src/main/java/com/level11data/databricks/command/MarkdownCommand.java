package com.level11data.databricks.command;


public class MarkdownCommand extends AbstractCommand {

    private String COMMAND_DIRECTIVE = "%md";

    public MarkdownCommand(String command){
        super(command);
    }

    public String getCommandWithDirective() {
        return getCommandWithDirective(COMMAND_DIRECTIVE);
    }

}
