package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;

public class ClusterSpec extends BaseCluster {

    public final String SparkVersionKey;
    public final String NodeTypeId;
    public final String DriverNodeTypeId;

    public final String Name;
    public final Integer NumWorkers;
    public final AutoScale AutoScale;
    public final Integer AutoTerminationMinutes;

    //public final List<Library> Libraries;  TODO Add Libraries

    public ClusterSpec(ClusterInfoDTO clusterInfoDTO) {
        super(clusterInfoDTO);

        SparkVersionKey = clusterInfoDTO.SparkVersionKey;
        NodeTypeId = clusterInfoDTO.NodeTypeId;
        DriverNodeTypeId = clusterInfoDTO.DriverNodeTypeId;

        Name = clusterInfoDTO.ClusterName;
        NumWorkers = clusterInfoDTO.NumWorkers;
        AutoTerminationMinutes = clusterInfoDTO.AutoTerminationMinutes;

        if(clusterInfoDTO.AutoScale != null) {
            AutoScale = new AutoScale(clusterInfoDTO.AutoScale);
        } else {
            AutoScale = null;
        }
    }
}
