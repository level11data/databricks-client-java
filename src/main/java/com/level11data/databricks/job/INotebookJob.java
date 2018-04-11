package com.level11data.databricks.job;

import com.level11data.databricks.job.run.INotebookJobRun;
import com.level11data.databricks.job.run.JobRunException;

import java.util.Map;

public interface INotebookJob extends IJob {

    INotebookJobRun run(Map<String,String> overrideParameters) throws JobRunException;
}
