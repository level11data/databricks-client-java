package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.Library;
import java.util.List;

public abstract class AutomatedJob extends Job {
    public final ClusterSpec ClusterSpec;

    protected AutomatedJob(JobsClient client,
                           Long jobId,
                           JobSettingsDTO jobSettingsDTO,
                           List<Library> libraries) throws JobConfigException {
        super(client, jobId, jobSettingsDTO, libraries);
        ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
    }

    protected AutomatedJob(JobsClient client,
                           Long jobId,
                           JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super(client, jobId, jobSettingsDTO, null);
        ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
    }

}
