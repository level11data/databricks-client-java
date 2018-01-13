package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;

import java.net.URISyntaxException;
import java.util.List;

public abstract class AutomatedJob extends Job {
    public final ClusterSpec ClusterSpec;

    protected AutomatedJob(JobsClient client,
                           long jobId,
                           JobSettingsDTO jobSettingsDTO,
                           List<Library> libraries) throws URISyntaxException, LibraryConfigException {
        super(client, jobId, jobSettingsDTO, libraries);
        ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
    }

    protected AutomatedJob(JobsClient client,
                           long jobId,
                           JobSettingsDTO jobSettingsDTO) throws URISyntaxException, LibraryConfigException {
        super(client, jobId, jobSettingsDTO, null);
        ClusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
    }

}
