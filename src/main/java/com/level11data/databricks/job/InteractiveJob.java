package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.entities.jobs.JobSettingsDTO;

public abstract class InteractiveJob extends Job {

    public final InteractiveCluster Cluster;

    protected InteractiveJob(JobsClient client, InteractiveCluster cluster, long jobId, JobSettingsDTO jobSettingsDTO) {
      super(client, jobId, jobSettingsDTO);
      Cluster = cluster;
    }
}
