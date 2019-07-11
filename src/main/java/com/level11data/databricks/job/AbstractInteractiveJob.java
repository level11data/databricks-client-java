package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.library.Library;

import java.util.List;

public abstract class AbstractInteractiveJob extends AbstractJob {
    private final JobsClient _client;
    private final InteractiveCluster _cluster;
    private ClusterSpec _clusterSpec;

    protected AbstractInteractiveJob(JobsClient client,
                                     InteractiveCluster cluster,
                                     Long jobId,
                                     JobSettingsDTO jobSettingsDTO,
                                     List<Library> libraries) throws JobConfigException{
        super(client, jobId, jobSettingsDTO, libraries);
        _client = client;
        _cluster = cluster;
    }

    public ClusterSpec getClusterSpec() throws JobConfigException {
        if(_clusterSpec == null) {
            try{
                ClusterInfoDTO clusterInfoDTO = _client.Session.getClustersClient().getCluster(_cluster.getId());
                _clusterSpec = new ClusterSpec(clusterInfoDTO);
            } catch(HttpException e) {
                throw new JobConfigException(e);
            } catch(ClusterConfigException e) {
                throw new JobConfigException(e);
            }
        }
        return _clusterSpec;
    }

    public InteractiveCluster getCluster() {
        return _cluster;
    }
}
