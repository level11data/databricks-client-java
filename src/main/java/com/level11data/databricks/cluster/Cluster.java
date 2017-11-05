package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.*;
import java.math.BigInteger;
import java.util.*;

public abstract class Cluster extends BaseCluster {
    private ClustersClient _client;
    private ClusterInfoDTO _clusterInfoDTO;

    public final String Id;
    public final SparkVersion SparkVersion;
    public final NodeType DefaultNodeType;
    public final Map<String, String> DefaultTags;
    public final String CreatorUserName;
    public final ServiceType CreatedBy;
    public final SparkNode Driver;
    public final Long SparkContextId;
    public final Integer JdbcPort;
    public final Date StartTime;

    protected Cluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);

        _client = client;
        _clusterInfoDTO = getClusterInfo();

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        Id = info.ClusterId;
        SparkVersion = initSparkVersion();
        DefaultNodeType = initNodeType();
        CreatorUserName = initCreatorUserName();
        CreatedBy = initCreatedBy();
        Driver = initDriver();
        SparkContextId = initSparkContextId();
        JdbcPort = initJdbcPort();
        StartTime = initStartTime();
        DefaultTags = Collections.unmodifiableMap(initDefaultTags());
    }

    private SparkVersion initSparkVersion() throws HttpException, ClusterConfigException {
        return _client.Session.getSparkVersionByKey(_clusterInfoDTO.SparkVersionKey);
    }

    private NodeType initNodeType() throws HttpException, ClusterConfigException {
        return _client.Session.getNodeTypeById(_clusterInfoDTO.NodeTypeId);
    }

    private String initCreatorUserName() throws HttpException {
        return _clusterInfoDTO.CreatorUserName;
    }

    private ServiceType initCreatedBy() throws HttpException {
        return ServiceType.valueOf(_clusterInfoDTO.ClusterCreatedBy);
    }

    private SparkNode initDriver() throws HttpException, ClusterConfigException {
        NodeType driverNodeType = _client.Session.getNodeTypeById(_clusterInfoDTO.DriverNodeTypeId);
        if(_clusterInfoDTO.Driver == null) {
            return null;
        } else {
            return new SparkNode(_clusterInfoDTO.Driver, driverNodeType);
        }
    }

    private Long initSparkContextId() throws HttpException {
        return _clusterInfoDTO.SparkContextId;
    }

    private Integer initJdbcPort() throws HttpException {
        return _clusterInfoDTO.JdbcPort;
    }

    private Date initStartTime() throws HttpException  {
        Long startTime;
        startTime = _clusterInfoDTO.StartTime;
        return new Date(startTime.longValue());
    }

    private Map<String, String> initDefaultTags() throws HttpException {
        if(_clusterInfoDTO.DefaultTags == null) {
            return getClusterInfo().DefaultTags;
        } else {
            return _clusterInfoDTO.DefaultTags;
        }
    }

    public ClusterState getState() throws HttpException {
        //Always make client request for this
        return ClusterState.valueOf(_client.getCluster(Id).State);
    }

    public String getStateMessage() throws HttpException {
        //Always make client request for this
        return _client.getCluster(Id).StateMessage;
    }

    public ArrayList<SparkNode> getExecutors() throws HttpException, ClusterConfigException {
        //Always make client request for this
        SparkNodeDTO[] nodeInfos =  _client.getCluster(Id).Executors;

        ArrayList<SparkNode> nodeList = new ArrayList<SparkNode>();

        if(nodeInfos != null) {
            for(SparkNodeDTO nodeInfo : nodeInfos) {
                nodeList.add(new SparkNode(nodeInfo, initNodeType()));
            }
        }
        return nodeList;
    }

    public BigInteger getTerminatedTime() throws HttpException  {
        //Always make client request for this
        return _client.getCluster((Id)).TerminatedTime;
    }

    public BigInteger getLastStateLossType() throws HttpException {
        //Always make client request for this
        return _client.getCluster((Id)).LastStateLossTime;
    }

    public BigInteger getLastActivityTime() throws HttpException  {
        //Always make client request for this
        return _client.getCluster((Id)).LastActivityTime;
    }

    public BigInteger getClusterMemoryMb() throws HttpException {
        //Always make client request for this
        return _client.getCluster((Id)).ClusterMemoryMb;
    }

    public BigInteger getClusterCores() throws HttpException {
        //Always make client request for this
        return _client.getCluster((Id)).ClusterCores;
    }

    public LogSyncStatus getLogStatus() throws HttpException {
        //Always make client request for this
        return new LogSyncStatus(_client.getCluster(Id).ClusterLogStatus);
    }

    public TerminationReasonDTO getTerminationReason() throws HttpException {
        //Always make client request for this
        return _client.getCluster(Id).TerminationReason;
    }

}
