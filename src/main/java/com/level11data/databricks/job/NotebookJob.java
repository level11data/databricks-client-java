package com.level11data.databricks.job;

import com.level11data.databricks.job.run.NotebookJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.workspace.Notebook;

import java.util.Map;

public interface NotebookJob extends Job {

    Notebook getNotebook();

    Map<String,String> getBaseParameters();

    NotebookJobRun run(Map<String,String> overrideParameters) throws JobRunException;
}
