package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;

public class ClusterSpec extends AbstractBaseCluster {

    private ClusterInfoDTO _clusterInfoDTO;

    //No Id since this is just a cluster specification

    //DTO will just have version key (if any); so no way to create a SparkVerion obj without Value
    public final String SparkVersionKey;

    //DTO will just have node type key (if any); so no way to create a NodeType obj without Value
    public final String NodeTypeId;

    //DTO will just have node type key (if any); so no way to create a NodeType obj without Value
    public final String DriverNodeTypeId;

    //AutomatedCluster cannot specify AutoTerminationMinutes; which is why this isn't on AbstractBaseCluster
    public final Integer AutoTerminationMinutes;

    public ClusterSpec(ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException {
        super(clusterInfoDTO);
        _clusterInfoDTO = clusterInfoDTO;

        SparkVersionKey = clusterInfoDTO.SparkVersionKey;  //could be null
        NodeTypeId = clusterInfoDTO.NodeTypeId;  //could be null
        DriverNodeTypeId = clusterInfoDTO.DriverNodeTypeId;  //could be null
        AutoTerminationMinutes = clusterInfoDTO.AutoTerminationMinutes;  //could be null
    }

    public ClusterInfoDTO getClusterInfo() {
        return _clusterInfoDTO;
    }
}
