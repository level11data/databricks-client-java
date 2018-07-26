package com.level11data.databricks.command;

public interface Command {
  String getCommand();

  String getCommandWithDirective();
}
