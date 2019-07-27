package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.cluster.Cluster;
import com.level11data.databricks.job.Job;
import com.level11data.databricks.job.TriggerType;
import com.level11data.databricks.library.AbstractLibrary;

import java.util.Date;
import java.util.List;

public interface JobRun {

    RunState getRunState() throws JobRunException;

    String getSparkContextId() throws JobRunException;

    Long getSetupDuration() throws JobRunException;

    Long getExecutionDuration() throws JobRunException;

    Long getCleanupDuration() throws JobRunException;

    Cluster getCluster() throws JobRunException;

    void cancel() throws JobRunException;

    Job getJob() throws JobRunException;

    long getJobId();

    long getRunId();

    String getCreatorUserName();

    long getNumberInJob();

    long getOriginalAttemptRunId(); //TODO convert this to a FK to the ParentJobRun

    //CronScheduleDTO getSchedule();

    TriggerType getTrigger();

    Date getStartTime();

    List<AbstractLibrary> getLibraries();

}
