package com.level11data.databricks.job.run;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.AutomatedCluster;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;

public abstract class AbstractAutomatedJobRun extends AbstractJobRun {
    private AutomatedCluster _cluster;
    private JobsClient _client;
    private boolean _clusterCreated = false;

    public final ClusterSpec NewClusterSpec;

    protected AbstractAutomatedJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;

        validateJobRun(runDTO);
        try{
            NewClusterSpec = new ClusterSpec(runDTO.ClusterSpec.NewCluster);
        } catch(ClusterConfigException e) {
            throw new JobRunException(e);
        }

    }

    public AutomatedCluster getCluster() throws JobRunException {
        if (_cluster == null) {
            try {
                RunDTO run = _client.getRun(this.getRunId());
                validateJobRun(run);
                if(_clusterCreated) {
                    ClustersClient clusterClient = new ClustersClient(_client.Session);
                    ClusterInfoDTO clusterInfo = clusterClient.getCluster(run.ClusterInstance.ClusterId);
                    _cluster = new AutomatedCluster(clusterClient, clusterInfo);
                }
            } catch(HttpException e) {
                throw new JobRunException(e);
            } catch(ClusterConfigException e) {
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
            if(runDTO.ClusterSpec.NewCluster == null) {
                throw new JobRunException("JobRun is not associated with an automated cluster");
            } else {
                return;
            }
        }
    }



}
