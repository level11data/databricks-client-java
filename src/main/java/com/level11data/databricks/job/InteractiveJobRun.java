package com.level11data.databricks.job;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.entities.jobs.RunDTO;

abstract public class InteractiveJobRun extends JobRun {

    private InteractiveCluster _cluster;

    protected InteractiveJobRun(JobsClient client, RunDTO runDTO) {
      super(client, runDTO);
    }

    public InteractiveCluster getCluster() throws HttpException, ClusterConfigException {
        if (_cluster == null) {
            RunDTO run = _client.getRun(this.RunId);
            if (run.ClusterInstance == null) {
                return null;
            } else {
                String clusterId = run.ClusterInstance.ClusterId;
                if (clusterId == null) {
                    return null;
                } else {
                    ClustersClient clusterClient = new ClustersClient(_client.Session);
                    ClusterInfoDTO clusterInfo = clusterClient.getCluster(clusterId);
                    _cluster = new InteractiveCluster(clusterClient, clusterInfo);
                }
            }
        }
        return _cluster;
    }
}
