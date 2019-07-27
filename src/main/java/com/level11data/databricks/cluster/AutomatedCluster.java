package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;

public class AutomatedCluster extends AbstractCluster implements Cluster {

    public AutomatedCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);
    }

    public int getAutoTerminationMinutes() {
        //Automated Clusters cannot be configured with auto-termination minutes
        return 0;
    }

}
