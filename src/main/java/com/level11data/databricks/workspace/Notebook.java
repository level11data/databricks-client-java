package com.level11data.databricks.workspace;

import com.level11data.databricks.job.builder.AutomatedNotebookJobBuilder;

public class Notebook {
    public final String Path;

    public Notebook(String workspacePath) {
        Path = workspacePath;
    }

}
