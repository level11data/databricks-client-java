package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.*;
import com.level11data.databricks.entities.clusters.TerminationReason;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

public class Cluster {
    private ClustersClient _client;
    private ClusterInfo _clusterInfo;
    private Boolean _isAutoScaling = false;
    private Boolean _clusterInfoRequested = false;

    public final String Id;
    public final String Name;
    public final Integer NumWorkers;
    public final AutoScale AutoScale;
    public final String SparkVersion;
    public final String NodeType;
    public final String DriverNodeType;
    public final AwsAttributes AwsAttributes;
    public final Integer AutoTerminationMinutes;
    public final Boolean ElasticDiskEnabled;
    public final Map<String, String> SparkConf;
    public final String[] SshPublicKeys;
    public final Map<String, String> CustomTags;
    public final ClusterLogConf ClusterLogConf;
    public final Map<String, String> SparkEnvironmentVariables;
    public final String CreatorUserName;
    //This should probably be an enum but the values aren't documented (JOB_LAUNCHER, THIRD_PARTY)
    public final String CreatedBy;
    public final SparkNode Driver;
    public final Float SparkContextId;
    public final Integer JdbcPort;
    public final BigInteger StartTime;
    public final Map<String, String> DefaultTags;

    /**
     * Represents a Databricks Cluster.
     *
     * Is instantiated by
     *   1. ClusterBuilder.create()
     *   2. DatabricksSession.getCluster() by Cluster Id
     *   3. Cluster.resize()
     *
     * @param client Databricks ClusterClient
     * @param info Databricks ClusterInfo POJO
     * @throws ClusterConfigException
     */
    public Cluster(ClustersClient client, ClusterInfo info) throws ClusterConfigException, HttpException {
        _client = client;
        _clusterInfo = info;

        //Validate that required fields are populated in the ClusterInfo
        validateClusterInfo(info);

        Id = info.ClusterId;
        Name = info.ClusterName;
        NumWorkers = info.NumWorkers;

        if(info.AutoScale != null){
            _isAutoScaling = true;
            AutoScale = new AutoScale(info.AutoScale);
        } else {
            AutoScale = null;
        }

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set if object was instantiated from ClusterBuilder.create()
        SparkVersion = getSparkVersion();
        NodeType = getNodeTypeId();
        DriverNodeType = getDriverNodeTypeId();
        AwsAttributes = getAwsAttributes();
        AutoTerminationMinutes = getAutoTerminationMinutes();
        ElasticDiskEnabled = getElasticDiskEnabled();
        SparkConf = getSparkConf();
        SshPublicKeys = getSshPublicKeys();
        CustomTags = getCustomTags();
        ClusterLogConf = getLogConf();
        SparkEnvironmentVariables = getSparkEnvironmentVariables();
        CreatorUserName = getCreatorUserName();
        CreatedBy = getCreatedBy();
        Driver = getDriver();
        SparkContextId = getSparkContextId();
        JdbcPort = getJdbcPort();
        StartTime = getStartTime();
        DefaultTags = getDefaultTags();
    }

    private void validateClusterInfo(ClusterInfo info) throws ClusterConfigException {
        if(info.ClusterId == null) {
            throw new ClusterConfigException("ClusterInfo Must Have ClusterId");
        }

        if(info.ClusterName == null) {
            throw new ClusterConfigException("ClusterInfo Must Have Name");
        }

        if(info.NumWorkers == null && info.AutoScale == null)  {
            throw new ClusterConfigException("ClusterInfo Must Have either NumWorkers OR AutoScale");
        }
    }

    private ClusterInfo getOrRequestClusterInfo(ClusterInfo info) throws HttpException {
        if(!_clusterInfoRequested) {
            _clusterInfo = _client.getCluster(Id);
            _clusterInfoRequested = true;
            return _clusterInfo;
        } else {
            return info;
        }
    }

    private String getSparkVersion() throws HttpException {
        if(_clusterInfo.SparkVersion == null) {
            return getOrRequestClusterInfo(_clusterInfo).SparkVersion;
        } else {
            return _clusterInfo.SparkVersion;
        }
    }

    private String getNodeTypeId() throws HttpException {
        if(_clusterInfo.NodeTypeId == null) {
            return getOrRequestClusterInfo(_clusterInfo).NodeTypeId;
        } else {
            return _clusterInfo.NodeTypeId;
        }
    }

    private String getDriverNodeTypeId() throws HttpException {
        if(_clusterInfo.DriverNodeTypeId == null) {
            return getOrRequestClusterInfo(_clusterInfo).DriverNodeTypeId;
        } else {
            return _clusterInfo.DriverNodeTypeId;
        }
    }

    private AwsAttributes getAwsAttributes() throws HttpException {
        if(_clusterInfo.AwsAttributes == null) {
            return new AwsAttributes(getOrRequestClusterInfo(_clusterInfo).AwsAttributes);
        } else {
            return new AwsAttributes(_clusterInfo.AwsAttributes);
        }
    }

    private Integer getAutoTerminationMinutes() throws HttpException  {
        if(_clusterInfo.AutoTerminationMinutes == null) {
            return getOrRequestClusterInfo(_clusterInfo).AutoTerminationMinutes;
        } else {
            return _clusterInfo.AutoTerminationMinutes;
        }
    }

    private Boolean getElasticDiskEnabled() throws HttpException {
        return getOrRequestClusterInfo(_clusterInfo).EnableElasticDisk;
    }

    private Map<String, String> getSparkConf() throws HttpException {
        if(_clusterInfo.SparkConf == null) {
            return getOrRequestClusterInfo(_clusterInfo).SparkConf;
        } else {
            return _clusterInfo.SparkConf;
        }
    }

    private String[] getSshPublicKeys() throws HttpException {
        if(_clusterInfo.SshPublicKeys == null) {
            return getOrRequestClusterInfo(_clusterInfo).SshPublicKeys;
        } else {
            return _clusterInfo.SshPublicKeys;
        }
    }

    private Map<String, String> getCustomTags() throws HttpException {
        if(_clusterInfo.CustomTags == null) {
            return getOrRequestClusterInfo(_clusterInfo).CustomTags;
        } else {
            return _clusterInfo.CustomTags;
        }
    }

    private ClusterLogConf getLogConf() throws HttpException {
        if(_clusterInfo.ClusterLogConf == null && _clusterInfoRequested) {
            return null;
        } else if(_clusterInfo.ClusterLogConf == null && !_clusterInfoRequested) {
            if(getOrRequestClusterInfo(_clusterInfo).ClusterLogConf == null) {
                return null;
            } else {
                return new ClusterLogConf(getOrRequestClusterInfo(_clusterInfo).ClusterLogConf);
            }
        } else {
            return new ClusterLogConf(_clusterInfo.ClusterLogConf);
        }
    }

    private Map<String, String> getSparkEnvironmentVariables() throws HttpException {
        if(_clusterInfo.SparkEnvironmentVariables == null) {
            return getOrRequestClusterInfo(_clusterInfo).SparkEnvironmentVariables;
        } else {
            return _clusterInfo.SparkEnvironmentVariables;
        }
    }

    private String getCreatorUserName() throws HttpException {
        if(_clusterInfo.CreatorUserName == null) {
            return getOrRequestClusterInfo(_clusterInfo).CreatorUserName;
        } else {
            return _clusterInfo.CreatorUserName;
        }
    }

    private String getCreatedBy() throws HttpException {
        if(_clusterInfo.ClusterCreatedBy == null) {
            return getOrRequestClusterInfo(_clusterInfo).ClusterCreatedBy;
        } else {
            return _clusterInfo.ClusterCreatedBy;
        }
    }

    private SparkNode getDriver() throws HttpException {
        if(_clusterInfo.Driver == null && _clusterInfoRequested) {
            return null;
        } else if(_clusterInfo.Driver == null && !_clusterInfoRequested) {
            if(getOrRequestClusterInfo(_clusterInfo).Driver == null) {
                return null;
            } else {
                return new SparkNode(getOrRequestClusterInfo(_clusterInfo).Driver);
            }
        } else {
            return new SparkNode(_clusterInfo.Driver);
        }
    }

    private Float getSparkContextId() throws HttpException {
        if(_clusterInfo.SparkContextId == null) {
            return getOrRequestClusterInfo(_clusterInfo).SparkContextId;
        } else {
            return _clusterInfo.SparkContextId;
        }
    }

    private Integer getJdbcPort() throws HttpException {
        if(_clusterInfo.JdbcPort == null) {
            return getOrRequestClusterInfo(_clusterInfo).JdbcPort;
        } else {
            return _clusterInfo.JdbcPort;
        }
    }

    private BigInteger getStartTime() throws HttpException  {
        if(_clusterInfo.StartTime == null) {
            return getOrRequestClusterInfo(_clusterInfo).StartTime;
        } else {
            return _clusterInfo.StartTime;
        }
    }

    private Map<String, String> getDefaultTags() throws HttpException {
        if(_clusterInfo.DefaultTags == null) {
            return getOrRequestClusterInfo(_clusterInfo).DefaultTags;
        } else {
            return _clusterInfo.DefaultTags;
        }
    }

    public ClusterInfo.ClusterState getState() throws HttpException {
        //Always make client request for this
        return _client.getCluster(Id).State;
    }

    public String getStateMessage() throws HttpException {
        //Always make client request for this
        return _client.getCluster(Id).StateMessage;
    }

    public SparkNode[] getExecutors() throws HttpException {
        //Always make client request for this
        com.level11data.databricks.entities.clusters.SparkNode[] nodeInfos =  _client.getCluster(Id).Executors;

        ArrayList<SparkNode> nodeList = new ArrayList<SparkNode>();

        for(com.level11data.databricks.entities.clusters.SparkNode nodeInfo : nodeInfos) {
            nodeList.add(new SparkNode(nodeInfo));
        }
        return nodeList.toArray(new SparkNode[nodeList.size()]);
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

    public TerminationReason getTerminationReason() throws HttpException {
        //Always make client request for this
        return _client.getCluster(Id).TerminationReason;
    }

    public void start() throws HttpException {
        _client.start(Id);
    }

    public void restart() throws HttpException {
        _client.reStart(Id);
    }

    public void terminate() throws HttpException {
        _client.delete(Id);
    }

    public Cluster resize(Integer numWorkers) throws ClusterConfigException, HttpException {
        if(_isAutoScaling) {
            throw new ClusterConfigException("Must Include New Min and Max Worker Values when Resizing an Autoscaling Cluster");
        }
        _client.resize(Id, numWorkers);

        ClusterInfo resizedClusterConfig = _clusterInfo;
        resizedClusterConfig.NumWorkers = numWorkers;
        return new Cluster(_client, resizedClusterConfig);
    }

    public Cluster resize(Integer minWorkers, Integer maxWorkers) throws ClusterConfigException, HttpException {
        if(!_isAutoScaling) {
            throw new ClusterConfigException("Must Only Include a Single Value When Resizing a Fixed Size Cluster");
        }
        _client.resize(Id, minWorkers, maxWorkers);

        ClusterInfo resizedClusterConfig = _clusterInfo;
        resizedClusterConfig.AutoScale.MinWorkers = minWorkers;
        resizedClusterConfig.AutoScale.MaxWorkers = maxWorkers;
        return new Cluster(_client, resizedClusterConfig);
    }
}
