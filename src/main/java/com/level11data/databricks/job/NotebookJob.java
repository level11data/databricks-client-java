package com.level11data.databricks.job;

import com.level11data.databricks.job.run.NotebookJobRun;
import com.level11data.databricks.job.run.JobRunException;

import java.util.Map;

public interface NotebookJob extends Job {

    NotebookJobRun run(Map<String,String> overrideParameters) throws JobRunException;
}
