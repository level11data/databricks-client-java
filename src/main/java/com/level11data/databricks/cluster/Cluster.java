package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.*;
import java.math.BigInteger;
import java.util.*;

public abstract class Cluster {
    private Boolean _clusterInfoRequested = false;
    private ClusterInfoDTO _clusterInfoDTO;
    private ClustersClient _client;

    public final String Id;
    public final SparkVersion SparkVersion;
    public final NodeType DefaultNodeType;
    public final AwsAttributes AwsAttributes;
    public final Boolean ElasticDiskEnabled;
    public final Map<String, String> SparkConf;
    public final List<String> SshPublicKeys;
    public final Map<String, String> CustomTags;
    public final ClusterLogConf ClusterLogConf;
    public final Map<String, String> SparkEnvironmentVariables;
    public final String CreatorUserName;
    //This should probably be an enum but the values aren't documented (JOB_LAUNCHER, THIRD_PARTY)
    public final String CreatedBy;  //TODO Change to Enum
    public final SparkNode Driver;
    public final Long SparkContextId;
    public final Integer JdbcPort;
    public final Date StartTime;
    public final Map<String, String> DefaultTags;

    protected Cluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        _client = client;
        _clusterInfoDTO = info;

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(info);

        Id = info.ClusterId;

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        SparkVersion = initSparkVersion();
        DefaultNodeType = initNodeType();
        AwsAttributes = initAwsAttributes();
        ElasticDiskEnabled = initElasticDiskEnabled();
        SparkConf = initSparkConf();
        SshPublicKeys = initSshPublicKeys();
        CustomTags = initCustomTags();
        ClusterLogConf = initLogConf();
        SparkEnvironmentVariables = initSparkEnvironmentVariables();
        CreatorUserName = initCreatorUserName();
        CreatedBy = initCreatedBy();
        Driver = initDriver();
        SparkContextId = initSparkContextId();
        JdbcPort = initJdbcPort();
        StartTime = initStartTime();
        DefaultTags = initDefaultTags();
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterId == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have ClusterId");
        }
    }

    protected ClusterInfoDTO getOrRequestClusterInfo(ClusterInfoDTO info) throws HttpException {
        if(!_clusterInfoRequested) {
            _clusterInfoDTO = _client.getCluster(Id);
            _clusterInfoRequested = true;
            return _clusterInfoDTO;
        } else {
            return info;
        }
    }

    private SparkVersion initSparkVersion() throws HttpException, ClusterConfigException {
        String sparkVersionKey = _clusterInfoDTO.SparkVersionKey == null ?
                getOrRequestClusterInfo(_clusterInfoDTO).SparkVersionKey :
                _clusterInfoDTO.SparkVersionKey;

        return _client.Session.getSparkVersionByKey(sparkVersionKey);
    }

    private NodeType initNodeType() throws HttpException, ClusterConfigException {
        if(_clusterInfoDTO.NodeTypeId == null) {
            return _client.Session.getNodeTypeById(getOrRequestClusterInfo(_clusterInfoDTO).NodeTypeId);
        } else {
            return _client.Session.getNodeTypeById(_clusterInfoDTO.NodeTypeId);
        }
    }

    private NodeType initDriverNodeType() throws HttpException, ClusterConfigException {
        if(_clusterInfoDTO.DriverNodeTypeId == null) {
            return _client.Session.getNodeTypeById(getOrRequestClusterInfo(_clusterInfoDTO).DriverNodeTypeId);
        } else {
            return _client.Session.getNodeTypeById(_clusterInfoDTO.DriverNodeTypeId);
        }
    }

    private AwsAttributes initAwsAttributes() throws HttpException {
        if(_clusterInfoDTO.AwsAttributes == null) {
            return new AwsAttributes(getOrRequestClusterInfo(_clusterInfoDTO).AwsAttributes);
        } else {
            return new AwsAttributes(_clusterInfoDTO.AwsAttributes);
        }
    }

    private Boolean initElasticDiskEnabled() throws HttpException {
        return getOrRequestClusterInfo(_clusterInfoDTO).EnableElasticDisk;
    }

    private Map<String, String> initSparkConf() throws HttpException {
        if(_clusterInfoDTO.SparkConf == null) {
            Map<String, String> sparkConfDTO = getOrRequestClusterInfo(_clusterInfoDTO).SparkConf;
            return sparkConfDTO == null ? new HashMap<String,String>() : sparkConfDTO;
        } else {
            Map<String, String> sparkConfDTO = _clusterInfoDTO.SparkConf;
            return sparkConfDTO == null ? new HashMap<String,String>() : sparkConfDTO;
        }
    }

    private List<String> initSshPublicKeys() throws HttpException {
        List<String> sshPublicKeysList = new ArrayList<String>();
        String[] sshPublicKeysDTO;
        if(_clusterInfoDTO.SshPublicKeys == null) {
            sshPublicKeysDTO = getOrRequestClusterInfo(_clusterInfoDTO).SshPublicKeys;
        } else {
            sshPublicKeysDTO =  _clusterInfoDTO.SshPublicKeys;
        }

        if(sshPublicKeysDTO != null) {
            for (String ssh : sshPublicKeysDTO) {
                sshPublicKeysList.add(ssh);
            }
        }
        return sshPublicKeysList;
    }

    private Map<String, String> initCustomTags() throws HttpException {
        if(_clusterInfoDTO.CustomTags == null) {
            Map<String, String> customTagsDTO = getOrRequestClusterInfo(_clusterInfoDTO).CustomTags;
            return customTagsDTO == null ? new HashMap<String,String>() : customTagsDTO;
        } else {
            Map<String, String> customTagsDTO =_clusterInfoDTO.CustomTags;
            return customTagsDTO == null ? new HashMap<String,String>() : customTagsDTO;
        }
    }

    private ClusterLogConf initLogConf() throws HttpException {
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

    private Map<String, String> initSparkEnvironmentVariables() throws HttpException {
        Map<String, String> sparkEvnVarDTO;

        if(_clusterInfoDTO.SparkEnvironmentVariables == null) {
            sparkEvnVarDTO =  getOrRequestClusterInfo(_clusterInfoDTO).SparkEnvironmentVariables;
        } else {
            sparkEvnVarDTO =  _clusterInfoDTO.SparkEnvironmentVariables;
        }
        return sparkEvnVarDTO == null ? new HashMap<String,String>() : sparkEvnVarDTO;
    }

    private String initCreatorUserName() throws HttpException {
        if(_clusterInfoDTO.CreatorUserName == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).CreatorUserName;
        } else {
            return _clusterInfoDTO.CreatorUserName;
        }
    }

    private String initCreatedBy() throws HttpException {
        if(_clusterInfoDTO.ClusterCreatedBy == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).ClusterCreatedBy;
        } else {
            return _clusterInfoDTO.ClusterCreatedBy;
        }
    }

    private SparkNode initDriver() throws HttpException, ClusterConfigException {
        NodeType driverNodeType = initDriverNodeType();
        if(_clusterInfoDTO.Driver == null && _clusterInfoRequested) {
            return null;
        } else if(_clusterInfoDTO.Driver == null && !_clusterInfoRequested) {
            if(getOrRequestClusterInfo(_clusterInfoDTO).Driver == null) {
                return null;
            } else {
                SparkNodeDTO driverNodeDTO = getOrRequestClusterInfo(_clusterInfoDTO).Driver;
                return new SparkNode(driverNodeDTO, driverNodeType);
            }
        } else {
            return new SparkNode(_clusterInfoDTO.Driver, driverNodeType);
        }
    }

    private Long initSparkContextId() throws HttpException {
        if(_clusterInfoDTO.SparkContextId == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).SparkContextId;
        } else {
            return _clusterInfoDTO.SparkContextId;
        }
    }

    private Integer initJdbcPort() throws HttpException {
        if(_clusterInfoDTO.JdbcPort == null) {
            return getOrRequestClusterInfo(_clusterInfoDTO).JdbcPort;
        } else {
            return _clusterInfoDTO.JdbcPort;
        }
    }

    private Date initStartTime() throws HttpException  {
        Long startTime;
        if(_clusterInfoDTO.StartTime == null) {
            startTime = getOrRequestClusterInfo(_clusterInfoDTO).StartTime;
        } else {
            startTime = _clusterInfoDTO.StartTime;
        }
        return new Date(startTime.longValue());
    }

    private Map<String, String> initDefaultTags() throws HttpException {
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
