package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.clusters.SparkNodeDTO;

import java.math.BigInteger;
import java.util.*;

public abstract class Cluster extends BaseCluster {
    private ClustersClient _client;

    public final String Id;
    public final SparkVersion SparkVersion;
    public final NodeType DefaultNodeType;
    public final String CreatorUserName;
    public final ServiceType CreatedBy;
    public final ClusterSource ClusterSource;
    public final SparkNode Driver;
    public final Long SparkContextId;
    public final Integer JdbcPort;
    public final Date StartTime;

    protected Cluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException {
        super(client, info);

        _client = client;

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        Id = info.ClusterId;
        SparkVersion = initSparkVersion();
        DefaultNodeType = initNodeType();
        CreatorUserName = initCreatorUserName();
        CreatedBy = initCreatedBy();
        ClusterSource = initClusterSource();
        Driver = initDriver();
        SparkContextId = initSparkContextId();
        JdbcPort = initJdbcPort();
        StartTime = initStartTime();
    }

    private SparkVersion initSparkVersion() throws ClusterConfigException {
        return _client.Session.getSparkVersionByKey(getClusterInfo().SparkVersionKey);
    }

    private NodeType initNodeType() throws ClusterConfigException {
        return _client.Session.getNodeTypeById(getClusterInfo().NodeTypeId);
    }

    private String initCreatorUserName() throws ClusterConfigException {
        return getClusterInfo().CreatorUserName;
    }

    private ServiceType initCreatedBy() throws ClusterConfigException {
        //Looks like this has been deprecated; possibly in favor of ClusterSource
        if(getClusterInfo().ClusterCreatedBy != null) {
            return ServiceType.valueOf(getClusterInfo().ClusterCreatedBy);
        } else {
            return null;
        }
    }

    private ClusterSource initClusterSource() throws ClusterConfigException {
        if(getClusterInfo().ClusterSource != null) {
            return ClusterSource.valueOf(getClusterInfo().ClusterSource);
        } else {
            return null;
        }
    }

    private SparkNode initDriver() throws ClusterConfigException {
        NodeType driverNodeType = _client.Session.getNodeTypeById(getClusterInfo().DriverNodeTypeId);
        if(getClusterInfo().Driver == null) {
            return null;
        } else {
            return new SparkNode(getClusterInfo().Driver, driverNodeType);
        }
    }

    private Long initSparkContextId() throws ClusterConfigException {
        return getClusterInfo().SparkContextId;
    }

    private Integer initJdbcPort() throws ClusterConfigException {
        return getClusterInfo().JdbcPort;
    }

    private Date initStartTime() throws ClusterConfigException  {
        Long startTime;
        startTime = getClusterInfo().StartTime;
        return new Date(startTime.longValue());
    }

    public ClusterState getState() throws ClusterConfigException {
        try {
            //Always make client request for this
            return ClusterState.valueOf(_client.getCluster(Id).State);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public String getStateMessage() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster(Id).StateMessage;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public ArrayList<SparkNode> getExecutors() throws ClusterConfigException {
        try {
            //Always make client request for this
            SparkNodeDTO[] nodeInfos =  _client.getCluster(Id).Executors;

            ArrayList<SparkNode> nodeList = new ArrayList<>();

            if(nodeInfos != null) {
                for(SparkNodeDTO nodeInfo : nodeInfos) {
                    nodeList.add(new SparkNode(nodeInfo, initNodeType()));
                }
            }
            return nodeList;
        } catch(HttpException e) {
           throw new ClusterConfigException(e);
        }
    }

    public BigInteger getTerminatedTime() throws ClusterConfigException  {
        try {
            //Always make client request for this
            return _client.getCluster((Id)).TerminatedTime;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getLastStateLossType() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster((Id)).LastStateLossTime;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getLastActivityTime() throws ClusterConfigException  {
        try {
            //Always make client request for this
            return _client.getCluster((Id)).LastActivityTime;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getClusterMemoryMb() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster((Id)).ClusterMemoryMb;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getClusterCores() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster((Id)).ClusterCores;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public LogSyncStatus getLogStatus() throws ClusterConfigException {
        try {
            //Always make client request for this
            return new LogSyncStatus(_client.getCluster(Id).ClusterLogStatus);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public TerminationReason getTerminationReason() throws ClusterConfigException {
        try {
            //Always make client request for this
            return new TerminationReason(_client.getCluster(Id).TerminationReason);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }

    }

}
