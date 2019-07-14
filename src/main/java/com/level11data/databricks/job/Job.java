package com.level11data.databricks.job;

import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.job.run.JobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.Library;

import java.util.Date;
import java.util.List;

public interface Job {

    Date getCreatedTime() throws JobConfigException;

    String getCreatorUserName() throws JobConfigException;

    void delete() throws JobConfigException;

    JobRun run() throws JobRunException;

    long getId();

    String getName();

    List<Library> getLibraries();

    EmailNotification getNotificationOnStart(); //change to ArrayList of email addresses (is there a type?)

    EmailNotification getNotificationOnSuccess(); //change to ArrayList of email addresses (is there a type?)

    EmailNotification getNotificationOnFailure(); //change to ArrayList of email addresses (is there a type?)

    Integer getMaxRetries();

    Integer getMinRetryIntervalMillis();

    boolean getRetryOnTimeout();

    //CronScheduleDTO getSchedule();

    Integer getMaxConcurrentRuns();

    Integer getTimeoutSeconds();

    ClusterSpec getClusterSpec() throws JobConfigException;

    //JobRun getLastJobRun();  //TODO add this
}
