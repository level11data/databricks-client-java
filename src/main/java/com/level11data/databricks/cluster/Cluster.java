package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.*;
import java.math.BigInteger;
import java.util.*;

public abstract class Cluster extends BaseCluster {
    private ClustersClient _client;

    public final String Id;
    public final SparkVersion SparkVersion;
    public final NodeType DefaultNodeType;
    public final String CreatorUserName;
    public final ServiceType CreatedBy;
    public final SparkNode Driver;
    public final Long SparkContextId;
    public final Integer JdbcPort;
    public final Date StartTime;

    protected Cluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);

        _client = client;

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
    }

    private SparkVersion initSparkVersion() throws HttpException, ClusterConfigException {
        return _client.Session.getSparkVersionByKey(getClusterInfo().SparkVersionKey);
    }

    private NodeType initNodeType() throws HttpException, ClusterConfigException {
        return _client.Session.getNodeTypeById(getClusterInfo().NodeTypeId);
    }

    private String initCreatorUserName() throws HttpException {
        return getClusterInfo().CreatorUserName;
    }

    private ServiceType initCreatedBy() throws HttpException {
        return ServiceType.valueOf(getClusterInfo().ClusterCreatedBy);
    }

    private SparkNode initDriver() throws HttpException, ClusterConfigException {
        NodeType driverNodeType = _client.Session.getNodeTypeById(getClusterInfo().DriverNodeTypeId);
        if(getClusterInfo().Driver == null) {
            return null;
        } else {
            return new SparkNode(getClusterInfo().Driver, driverNodeType);
        }
    }

    private Long initSparkContextId() throws HttpException {
        return getClusterInfo().SparkContextId;
    }

    private Integer initJdbcPort() throws HttpException {
        return getClusterInfo().JdbcPort;
    }

    private Date initStartTime() throws HttpException  {
        Long startTime;
        startTime = getClusterInfo().StartTime;
        return new Date(startTime.longValue());
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

        ArrayList<SparkNode> nodeList = new ArrayList<>();

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
