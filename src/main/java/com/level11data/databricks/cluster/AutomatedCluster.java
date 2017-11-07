package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;

public class AutomatedCluster extends Cluster {
    //private Boolean _isAutoScaling = false;

    public final String Name;
    public final Integer NumWorkers;
    //public final AutoScale AutoScale;

    public AutomatedCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(info);

        Name = info.ClusterName;
        NumWorkers = info.NumWorkers;

        /** Autoscaling is not yet supported on automated clusters
        if(info.AutoScale != null){
            _isAutoScaling = true;
            AutoScale = new AutoScale(info.AutoScale);
        } else {
            AutoScale = null;
        }
        **/
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterName == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have Name");
        }

        /** Autoscaling is not yet supported on automated clusters
        if(info.NumWorkers == null && info.AutoScale == null)  {
            throw new ClusterConfigException("ClusterInfoDTO Must Have either NumWorkers OR AutoScaleDTO");
        }
        **/
    }

}
