package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.clusters.SparkNodeDTO;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.InstancePoolConfigException;

import java.math.BigInteger;
import java.util.Date;
import java.util.ArrayList;

public abstract class AbstractCluster extends AbstractBaseCluster implements Cluster {
    private final ClustersClient _client;
    private final String _id;
    private final SparkVersion _sparkVersion;
    private final NodeType _defaultNodeType;
    private final String _creatorUserName;
    private final ServiceType _createdBy;
    private final ClusterSource _clusterSource;
    private final SparkNode _driver;
    private final Long _sparkContextId;
    private final Integer _jdbcPort;
    private final Date _startTime;
    private final InstancePool _instancePool;

    protected AbstractCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException {
        super(client, info);
        validateClusterInfo(info);

        _client = client;

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        _id = info.ClusterId;
        _sparkVersion = initSparkVersion();
        _defaultNodeType = initNodeType();
        _creatorUserName = initCreatorUserName();
        _createdBy = initCreatedBy();
        _clusterSource = initClusterSource();
        _driver = initDriver();
        _sparkContextId = initSparkContextId();
        _jdbcPort = initJdbcPort();
        _startTime = initStartTime();

        try{
            if(info.InstancePoolId != null) {
                _instancePool = _client.Session.getInstancePool(info.InstancePoolId);
            } else {
                _instancePool = null;
            }
        } catch(InstancePoolConfigException e) {
            throw new ClusterConfigException(e);
        }
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if (info.ClusterId == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have ClusterId");
        }
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
            return ClusterState.valueOf(_client.getCluster(_id).State);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public String getStateMessage() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster(_id).StateMessage;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public ArrayList<SparkNode> getExecutors() throws ClusterConfigException {
        try {
            //Always make client request for this
            SparkNodeDTO[] nodeInfos =  _client.getCluster(_id).Executors;

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
            return _client.getCluster((_id)).TerminatedTime;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getLastStateLossType() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster((_id)).LastStateLossTime;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getLastActivityTime() throws ClusterConfigException  {
        try {
            //Always make client request for this
            return _client.getCluster((_id)).LastActivityTime;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getClusterMemoryMb() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster((_id)).ClusterMemoryMb;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public BigInteger getClusterCores() throws ClusterConfigException {
        try {
            //Always make client request for this
            return _client.getCluster((_id)).ClusterCores;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public LogSyncStatus getLogStatus() throws ClusterConfigException {
        try {
            //Always make client request for this
            return new LogSyncStatus(_client.getCluster(_id).ClusterLogStatus);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public TerminationReason getTerminationReason() throws ClusterConfigException {
        try {
            //Always make client request for this
            return new TerminationReason(_client.getCluster(_id).TerminationReason);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public String getId() {
        return _id;
    }

    public SparkVersion getSparkVersion() {
        return _sparkVersion;
    }

    public NodeType getDefaultNodeType() {
        return _defaultNodeType;
    }

    public String getCreatorUserName() {
        return _creatorUserName;
    }

    public ServiceType getCreatedBy() {
        return _createdBy;
    }

    public ClusterSource getClusterSource() {
        return _clusterSource;
    }

    public SparkNode getDriver() {
        return _driver;
    }

    public long getSparkContextId() {
        return _sparkContextId;
    }

    public Integer getJdbcPort() {
        return _jdbcPort;
    }

    public Date getStartTime() {
        return _startTime;
    }

    public InstancePool getInstancePool() {
        return _instancePool;
    }
}
