package com.level11data.databricks.job.run;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;

abstract public class AbstractInteractiveJobRun extends AbstractJobRun {
    private InteractiveCluster _cluster;
    private JobsClient _client;
    private boolean _clusterCreated = false;

    protected AbstractInteractiveJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;

        validateJobRun(runDTO);
    }

    public InteractiveCluster getCluster() throws JobRunException {
        if (_cluster == null) {
            try {
                RunDTO run = _client.getRun(this.getRunId());
                validateJobRun(run);
                ClustersClient clusterClient = new ClustersClient(_client.Session);
                ClusterInfoDTO clusterInfo = clusterClient.getCluster(run.ClusterInstance.ClusterId);
                _cluster = new InteractiveCluster(clusterClient, clusterInfo);
            } catch (HttpException e) {
                throw new JobRunException(e);
            } catch (ClusterConfigException e) {
                throw new JobRunException(e);
            }
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
