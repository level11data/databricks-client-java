package com.level11data.databricks.job;

import com.level11data.databricks.job.run.JobRun;
import com.level11data.databricks.job.run.JobRunException;

import java.util.Date;

public interface Job {

    Date getCreatedTime() throws JobConfigException;

    String getCreatorUserName() throws JobConfigException;

    void delete() throws JobConfigException;

    JobRun run() throws JobRunException;

}
