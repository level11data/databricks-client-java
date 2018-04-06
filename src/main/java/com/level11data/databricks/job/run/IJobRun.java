package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;

public interface IJobRun {

    RunState getRunState() throws HttpException;

    String getSparkContextId() throws HttpException;

    Long getSetupDuration() throws HttpException;

    Long getExecutionDuration() throws HttpException;

    Long getCleanupDuration() throws HttpException;

    //TODO change this to return an Interface; ICluster
    //Cluster getCluster() throws JobRunException;

    String getOutput() throws JobRunException;
}
