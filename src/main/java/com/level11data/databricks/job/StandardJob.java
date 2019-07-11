package com.level11data.databricks.job;

import com.level11data.databricks.job.run.JobRun;
import com.level11data.databricks.job.run.JobRunException;

import java.util.List;

public interface StandardJob extends Job {

    String[] getParameters();

    JobRun run(List<String> overrideParameters) throws JobRunException;
}
