package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.AutomatedCluster;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.Library;
import java.util.List;

public abstract class AbstractAutomatedJob extends AbstractJob {
    private final ClusterSpec _clusterSpec;

    protected AbstractAutomatedJob(JobsClient client,
                                   Long jobId,
                                   JobSettingsDTO jobSettingsDTO,
                                   List<Library> libraries) throws JobConfigException {
        super(client, jobId, jobSettingsDTO, libraries);
        try {
            _clusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
        } catch(ClusterConfigException e) {
            throw new JobConfigException(e);
        }

    }

    protected AbstractAutomatedJob(JobsClient client,
                                   Long jobId,
                                   JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super(client, jobId, jobSettingsDTO, null);
        try {
            _clusterSpec = new ClusterSpec(jobSettingsDTO.NewCluster);
        } catch(ClusterConfigException e) {
            throw new JobConfigException(e);
        }
    }

    public ClusterSpec getClusterSpec() {
        return _clusterSpec;
    }
}
