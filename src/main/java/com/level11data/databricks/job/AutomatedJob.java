package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.entities.jobs.JobSettingsDTO;

public abstract class AutomatedJob extends Job {
    public final ClusterSpec ClusterSpec;

    protected AutomatedJob(JobsClient client, long jobId, JobSettingsDTO jobSettingsDTO) {
        super(client, jobId, jobSettingsDTO);
        ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
    }


}
