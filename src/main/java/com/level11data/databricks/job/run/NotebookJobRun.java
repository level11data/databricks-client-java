package com.level11data.databricks.job.run;

import com.level11data.databricks.workspace.Notebook;

import java.util.Map;

public interface NotebookJobRun extends JobRun {

    String getOutput() throws JobRunException;

    Notebook getNotebook();

    Map<String,String> getBaseParameters();

    Map<String,String> getOverridingParameters();

}
