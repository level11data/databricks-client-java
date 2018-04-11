package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.Library;

import java.util.List;

public abstract class AbstractInteractiveJob extends AbstractJob {

    public final InteractiveCluster Cluster;

    protected AbstractInteractiveJob(JobsClient client,
                                     InteractiveCluster cluster,
                                     Long jobId,
                                     JobSettingsDTO jobSettingsDTO,
                                     List<Library> libraries) throws JobConfigException{
        super(client, jobId, jobSettingsDTO, libraries);
        Cluster = cluster;
    }
}
