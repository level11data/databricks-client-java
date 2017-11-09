package com.level11data.databricks.job;

import com.level11data.databricks.ClustersClient;
import com.level11data.databricks.HttpException;
import com.level11data.databricks.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.entities.jobs.RunDTO;

abstract public class InteractiveJobRun extends JobRun {
    private InteractiveCluster _cluster;
    private JobsClient _client;
    private boolean _clusterCreated = false;

    protected InteractiveJobRun(JobsClient client, RunDTO runDTO) throws JobRunException{
        super(client, runDTO);
        _client = client;

        validateJobRun(runDTO);
    }

    public InteractiveCluster getCluster() throws HttpException, ClusterConfigException, JobRunException {
        if (_cluster == null) {
            RunDTO run = _client.getRun(this.RunId);
            validateJobRun(run);
            ClustersClient clusterClient = new ClustersClient(_client.Session);
            ClusterInfoDTO clusterInfo = clusterClient.getCluster(run.ClusterInstance.ClusterId);
            _cluster = new InteractiveCluster(clusterClient, clusterInfo);
        }
        return _cluster;
    }

    private void validateJobRun(RunDTO runDTO) throws JobRunException {
        if (runDTO.ClusterInstance == null) {
            _clusterCreated = false;
            return;
        } else if(runDTO.ClusterInstance.ClusterId == null) {
            _clusterCreated = false;
            return;
        } else {
            _clusterCreated = true;
            if(runDTO.ClusterSpec.ExistingClusterId == null) {
                throw new JobRunException("JobRun is not associated with an interactive cluster");
            } else {
                return;
            }
        }
    }
}
