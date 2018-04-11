package com.level11data.databricks.job;

import com.level11data.databricks.job.run.IJobRun;
import com.level11data.databricks.job.run.JobRunException;

import java.util.List;

public interface IStandardJob extends IJob {

    IJobRun run(List<String> overrideParameters) throws JobRunException;
}
