package com.level11data.databricks.job;

import com.level11data.databricks.job.run.IJobRun;
import com.level11data.databricks.job.run.JobRunException;

import java.util.Date;

public interface IJob {

    Date getCreatedTime() throws JobConfigException;

    String getCreatorUserName() throws JobConfigException;

    void delete() throws JobConfigException;

    IJobRun run() throws JobRunException;

}
