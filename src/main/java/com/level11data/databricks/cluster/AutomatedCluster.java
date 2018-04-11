package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;

public class AutomatedCluster extends AbstractCluster implements Cluster {

    public AutomatedCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(info);
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.AutoTerminationMinutes != null) {
            throw new ClusterConfigException("AutomatedCluster ClusterInfoDTO Cannot Have AutoTerminationMinutes set");
        }
    }

}
