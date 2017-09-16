package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

public class Cluster {
    private ClustersClient _client;
    private ClusterInfoDTO _clusterInfoDTO;
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
     * @param info Databricks ClusterInfoDTO POJO
     * @throws ClusterConfigException
     */
    public Cluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        _client = client;
        _clusterInfoDTO = info;

        //Validate that required fields are populated in the ClusterInfoDTO
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

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterId == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have ClusterId");
        }

        if(info.ClusterName == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have Name");
        }

        if(info.NumWorkers == null && info.AutoScale == null)  {
            throw new ClusterConfigException("ClusterInfoDTO Must Have either NumWorkers OR AutoScaleDTO");
        }
    }

    private ClusterInfoDTO getOrRequestClusterInfo(ClusterInfoDTO info) throws HttpException {
        if(!_clusterInfoRequested) {
            _clusterInfoDTO = _client.getCluster(Id);
            _clusterInfoRequested = true;
            return _clusterInfoDTO;
        } else {
            return info;
        }
    }

    private String getSparkVersion() throws HttpException {
        if(_clusterInfoDTO.SparkVersion == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).SparkVersion;
        } else {
            return _clusterInfoDTO.SparkVersion;
        }
    }

    private String getNodeTypeId() throws HttpException {
        if(_clusterInfoDTO.NodeTypeId == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).NodeTypeId;
        } else {
            return _clusterInfoDTO.NodeTypeId;
        }
    }

    private String getDriverNodeTypeId() throws HttpException {
        if(_clusterInfoDTO.DriverNodeTypeId == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).DriverNodeTypeId;
        } else {
            return _clusterInfoDTO.DriverNodeTypeId;
        }
    }

    private AwsAttributes getAwsAttributes() throws HttpException {
        if(_clusterInfoDTO.AwsAttributes == null) {
            return new AwsAttributes(getOrRequestClusterInfo(_clusterInfoDTO).AwsAttributes);
        } else {
            return new AwsAttributes(_clusterInfoDTO.AwsAttributes);
        }
    }

    private Integer getAutoTerminationMinutes() throws HttpException  {
        if(_clusterInfoDTO.AutoTerminationMinutes == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).AutoTerminationMinutes;
        } else {
            return _clusterInfoDTO.AutoTerminationMinutes;
        }
    }

    private Boolean getElasticDiskEnabled() throws HttpException {
        return getOrRequestClusterInfo(_clusterInfoDTO).EnableElasticDisk;
    }

    private Map<String, String> getSparkConf() throws HttpException {
        if(_clusterInfoDTO.SparkConf == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).SparkConf;
        } else {
            return _clusterInfoDTO.SparkConf;
        }
    }

    private String[] getSshPublicKeys() throws HttpException {
        if(_clusterInfoDTO.SshPublicKeys == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).SshPublicKeys;
        } else {
            return _clusterInfoDTO.SshPublicKeys;
        }
    }

    private Map<String, String> getCustomTags() throws HttpException {
        if(_clusterInfoDTO.CustomTags == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).CustomTags;
        } else {
            return _clusterInfoDTO.CustomTags;
        }
    }

    private ClusterLogConf getLogConf() throws HttpException {
        if(_clusterInfoDTO.ClusterLogConf == null && _clusterInfoRequested) {
            return null;
        } else if(_clusterInfoDTO.ClusterLogConf == null && !_clusterInfoRequested) {
            if(getOrRequestClusterInfo(_clusterInfoDTO).ClusterLogConf == null) {
                return null;
            } else {
                return new ClusterLogConf(getOrRequestClusterInfo(_clusterInfoDTO).ClusterLogConf);
            }
        } else {
            return new ClusterLogConf(_clusterInfoDTO.ClusterLogConf);
        }
    }

    private Map<String, String> getSparkEnvironmentVariables() throws HttpException {
        if(_clusterInfoDTO.SparkEnvironmentVariables == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).SparkEnvironmentVariables;
        } else {
            return _clusterInfoDTO.SparkEnvironmentVariables;
        }
    }

    private String getCreatorUserName() throws HttpException {
        if(_clusterInfoDTO.CreatorUserName == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).CreatorUserName;
        } else {
            return _clusterInfoDTO.CreatorUserName;
        }
    }

    private String getCreatedBy() throws HttpException {
        if(_clusterInfoDTO.ClusterCreatedBy == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).ClusterCreatedBy;
        } else {
            return _clusterInfoDTO.ClusterCreatedBy;
        }
    }

    private SparkNode getDriver() throws HttpException {
        if(_clusterInfoDTO.Driver == null && _clusterInfoRequested) {
            return null;
        } else if(_clusterInfoDTO.Driver == null && !_clusterInfoRequested) {
            if(getOrRequestClusterInfo(_clusterInfoDTO).Driver == null) {
                return null;
            } else {
                return new SparkNode(getOrRequestClusterInfo(_clusterInfoDTO).Driver);
            }
        } else {
            return new SparkNode(_clusterInfoDTO.Driver);
        }
    }

    private Float getSparkContextId() throws HttpException {
        if(_clusterInfoDTO.SparkContextId == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).SparkContextId;
        } else {
            return _clusterInfoDTO.SparkContextId;
        }
    }

    private Integer getJdbcPort() throws HttpException {
        if(_clusterInfoDTO.JdbcPort == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).JdbcPort;
        } else {
            return _clusterInfoDTO.JdbcPort;
        }
    }

    private BigInteger getStartTime() throws HttpException  {
        if(_clusterInfoDTO.StartTime == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).StartTime;
        } else {
            return _clusterInfoDTO.StartTime;
        }
    }

    private Map<String, String> getDefaultTags() throws HttpException {
        if(_clusterInfoDTO.DefaultTags == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).DefaultTags;
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

    public ArrayList<SparkNode> getExecutors() throws HttpException {
        //Always make client request for this
        SparkNodeDTO[] nodeInfos =  _client.getCluster(Id).Executors;

        ArrayList<SparkNode> nodeList = new ArrayList<SparkNode>();

        if(nodeInfos != null) {
            for(SparkNodeDTO nodeInfo : nodeInfos) {
                nodeList.add(new SparkNode(nodeInfo));
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

        ClusterInfoDTO resizedClusterConfig = _clusterInfoDTO;
        resizedClusterConfig.NumWorkers = numWorkers;
        return new Cluster(_client, resizedClusterConfig);
    }

    public Cluster resize(Integer minWorkers, Integer maxWorkers) throws ClusterConfigException, HttpException {
        if(!_isAutoScaling) {
            throw new ClusterConfigException("Must Only Include a Single Value When Resizing a Fixed Size Cluster");
        }
        _client.resize(Id, minWorkers, maxWorkers);

        ClusterInfoDTO resizedClusterConfig = _clusterInfoDTO;
        resizedClusterConfig.AutoScale.MinWorkers = minWorkers;
        resizedClusterConfig.AutoScale.MaxWorkers = maxWorkers;
        return new Cluster(_client, resizedClusterConfig);
    }
}
