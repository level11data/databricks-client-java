package com.level11data.databricks.job.run;

public interface NotebookJobRun extends JobRun {

    String getOutput() throws JobRunException;

}
