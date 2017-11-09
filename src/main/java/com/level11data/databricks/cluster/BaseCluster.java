package com.level11data.databricks.cluster;


import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;

import java.util.*;

public abstract class BaseCluster {
    private Boolean _clusterInfoRequested = false;
    private ClusterInfoDTO _clusterInfoDTO;
    private ClustersClient _client;
    private String _clusterId;

    public final AwsAttributes AwsAttributes;
    public final Boolean ElasticDiskEnabled;
    public final Map<String, String> SparkConf;
    public final List<String> SshPublicKeys;
    public final Map<String, String> DefaultTags;
    public final Map<String, String> CustomTags;
    public final ClusterLogConf ClusterLogConf;
    public final Map<String, String> SparkEnvironmentVariables;

    protected BaseCluster(ClusterInfoDTO clusterInfoDTO) {
        _clusterInfoDTO = clusterInfoDTO;
        _clusterId = clusterInfoDTO.ClusterId;

        AwsAttributes = clusterInfoDTO.AwsAttributes == null ? null : new AwsAttributes(clusterInfoDTO.AwsAttributes);
        ElasticDiskEnabled = clusterInfoDTO.EnableElasticDisk;

        HashMap<String,String> sparkConfMap = new HashMap<>();
        if(clusterInfoDTO.SparkConf != null) {
            sparkConfMap.putAll(clusterInfoDTO.SparkConf);
        }
        SparkConf = Collections.unmodifiableMap(sparkConfMap);

        ArrayList<String> sshKeyList = new ArrayList<>();
        if(clusterInfoDTO.SshPublicKeys != null) {
            for (String sshPublicKey : clusterInfoDTO.SshPublicKeys) {
                sshKeyList.add(sshPublicKey);
            }
        }
        SshPublicKeys = Collections.unmodifiableList(sshKeyList);

        HashMap<String,String> defaultTagsMap = new HashMap<>();
        if(clusterInfoDTO.DefaultTags != null) {
            defaultTagsMap.putAll(clusterInfoDTO.DefaultTags);
        }
        DefaultTags = Collections.unmodifiableMap(defaultTagsMap);

        HashMap<String,String> customTagsMap = new HashMap<>();
        if(clusterInfoDTO.CustomTags != null) {
            customTagsMap.putAll(clusterInfoDTO.CustomTags);
        }
        CustomTags = Collections.unmodifiableMap(customTagsMap);

        ClusterLogConf = clusterInfoDTO.ClusterLogConf == null
                ? null : new ClusterLogConf(clusterInfoDTO.ClusterLogConf);

        HashMap<String,String> sparkEnvVarMap = new HashMap<>();
        if(clusterInfoDTO.SparkEnvironmentVariables != null) {
            sparkEnvVarMap.putAll(clusterInfoDTO.SparkEnvironmentVariables);
        }
        SparkEnvironmentVariables = Collections.unmodifiableMap(sparkEnvVarMap);
    }

    protected BaseCluster(ClustersClient client, ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException, HttpException {
        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(clusterInfoDTO);

        _client = client;
        _clusterInfoDTO = clusterInfoDTO;
        _clusterId = clusterInfoDTO.ClusterId;

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        AwsAttributes = initAwsAttributes();
        ElasticDiskEnabled = initElasticDiskEnabled();
        SparkConf = Collections.unmodifiableMap(initSparkConf());
        SshPublicKeys = Collections.unmodifiableList(initSshPublicKeys());
        DefaultTags = Collections.unmodifiableMap(initDefaultTags());
        CustomTags = Collections.unmodifiableMap(initCustomTags());
        SparkEnvironmentVariables = Collections.unmodifiableMap(initSparkEnvironmentVariables());
        ClusterLogConf = initLogConf();
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterId == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have ClusterId");
        }
    }

    protected ClusterInfoDTO getClusterInfo() throws HttpException {
        if(_client == null) {
            return _clusterInfoDTO;
        } else {
            if(!_clusterInfoRequested) {
                _clusterInfoDTO = _client.getCluster(_clusterId);
                _clusterInfoRequested = true;
                return _clusterInfoDTO;
            } else {
                return _clusterInfoDTO;
            }
        }
    }

    private AwsAttributes initAwsAttributes() throws HttpException {
        if(getClusterInfo().AwsAttributes == null) {
            return null;
        } else {
            return new AwsAttributes(getClusterInfo().AwsAttributes);
        }
    }

    private Boolean initElasticDiskEnabled() throws HttpException {
        return getClusterInfo().EnableElasticDisk;
    }

    private Map<String, String> initSparkConf() throws HttpException {
        if(getClusterInfo().SparkConf == null) {
            return new HashMap<String,String>();
        } else {
            return getClusterInfo().SparkConf;
        }
    }

    private List<String> initSshPublicKeys() throws HttpException {
        if(getClusterInfo().SshPublicKeys == null) {
            return new ArrayList<>();
        } else {
            List<String> sshPublicKeysList = new ArrayList<>();
            for (String ssh : getClusterInfo().SshPublicKeys) {
                sshPublicKeysList.add(ssh);
            }
            return sshPublicKeysList;
        }
    }

    private Map<String, String> initCustomTags() throws HttpException {
        if(getClusterInfo().CustomTags == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().CustomTags;
        }
    }

    private ClusterLogConf initLogConf() throws HttpException {
        if(getClusterInfo().ClusterLogConf == null) {
            return null;
        } else {
            return new ClusterLogConf(getClusterInfo().ClusterLogConf);
        }
    }

    private Map<String, String> initSparkEnvironmentVariables() throws HttpException {
        if(getClusterInfo().SparkEnvironmentVariables == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().SparkEnvironmentVariables;
        }
    }

    private Map<String, String> initDefaultTags() throws HttpException {
        if(getClusterInfo().DefaultTags == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().DefaultTags;
        }
    }



}
