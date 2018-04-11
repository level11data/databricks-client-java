package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.ILibrary;
import java.util.List;

public abstract class AutomatedJob extends Job {
    public final ClusterSpec ClusterSpec;

    protected AutomatedJob(JobsClient client,
                           Long jobId,
                           JobSettingsDTO jobSettingsDTO,
                           List<ILibrary> libraries) throws JobConfigException {
        super(client, jobId, jobSettingsDTO, libraries);
        try {
            ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
        } catch(ClusterConfigException e) {
            throw new JobConfigException(e);
        }

    }

    protected AutomatedJob(JobsClient client,
                           Long jobId,
                           JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super(client, jobId, jobSettingsDTO, null);
        try {
            ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
        } catch(ClusterConfigException e) {
            throw new JobConfigException(e);
        }
    }

}
