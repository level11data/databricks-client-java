package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.cluster.Cluster;

public interface JobRun {

    RunState getRunState() throws JobRunException;

    String getSparkContextId() throws JobRunException;

    Long getSetupDuration() throws JobRunException;

    Long getExecutionDuration() throws JobRunException;

    Long getCleanupDuration() throws JobRunException;

    Cluster getCluster() throws JobRunException;

    void cancel() throws JobRunException;
}
