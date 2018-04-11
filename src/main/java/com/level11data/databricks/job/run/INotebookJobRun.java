package com.level11data.databricks.job.run;

public interface INotebookJobRun extends IJobRun {

    String getOutput() throws JobRunException;

}
