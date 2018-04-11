package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.ILibrary;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;

import java.net.URISyntaxException;
import java.util.List;

public abstract class InteractiveJob extends Job {

    public final InteractiveCluster Cluster;

    protected InteractiveJob(JobsClient client,
                             InteractiveCluster cluster,
                             Long jobId,
                             JobSettingsDTO jobSettingsDTO,
                             List<ILibrary> libraries) throws JobConfigException{
        super(client, jobId, jobSettingsDTO, libraries);
        Cluster = cluster;
    }
}
