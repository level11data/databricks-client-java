package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;

public class ClusterSpec extends AbstractBaseCluster {

    private ClusterInfoDTO _clusterInfoDTO;

    //No Id since this is just a cluster specification

    //DTO will just have version key (if any); so no way to create a SparkVerion obj without Value
    private final String _sparkVersionKey;

    //DTO will just have node type key (if any); so no way to create a NodeType obj without Value
    private final String _nodeTypeId;

    //DTO will just have node type key (if any); so no way to create a NodeType obj without Value
    private final String _driverNodeTypeId;

    //AutomatedCluster cannot specify AutoTerminationMinutes; which is why this isn't on AbstractBaseCluster
    private final Integer _autoTerminationMinutes;

    private final String _InstancePoolId;

    /**
     *  A ClusterSpec includes all of the attributes required to create a cluster, but does not represent an
     *  instance of a cluster, and therefore has no Id.  A ClusterSpec is associated with a Job, whereas a JobRun
     *  would be associated with a Cluster.
     *
     * @param clusterInfoDTO Data Transfer Object representing ClusterInfo
     *                       https://docs.databricks.com/api/latest/clusters.html#request-structure
     * @throws ClusterConfigException
     */
    public ClusterSpec(ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException {
        super(clusterInfoDTO);
        _clusterInfoDTO = clusterInfoDTO;

        _sparkVersionKey = clusterInfoDTO.SparkVersionKey;  //could be null
        _nodeTypeId = clusterInfoDTO.NodeTypeId;  //could be null
        _driverNodeTypeId = clusterInfoDTO.DriverNodeTypeId;  //could be null
        _autoTerminationMinutes = clusterInfoDTO.AutoTerminationMinutes;  //could be null
        _InstancePoolId = clusterInfoDTO.InstancePoolId; //could be null
    }

    public ClusterInfoDTO getClusterInfo() {
        return _clusterInfoDTO;
    }

    public String getSparkVersionKey() {
        return _sparkVersionKey;
    }

    public String getNodeTypeId() {
        return _nodeTypeId;
    }

    public String getDriverNodeTypeId() {
        return _driverNodeTypeId;
    }

    public Integer getAutoTerminationMinutes() {
        return _autoTerminationMinutes;
    }

    public String getInstancePoolId() {
        return _InstancePoolId;
    }

}
