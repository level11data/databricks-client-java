package com.level11data.databricks.cluster;


import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;

import java.util.*;

public abstract class BaseCluster {
    private Boolean _clusterInfoRequested = false;
    private ClusterInfoDTO _clusterInfoDTO;
    private ClustersClient _client;

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

        AwsAttributes = clusterInfoDTO.AwsAttributes == null ? null : new AwsAttributes(clusterInfoDTO.AwsAttributes);
        ElasticDiskEnabled = clusterInfoDTO.EnableElasticDisk;
        SparkConf = Collections.unmodifiableMap(clusterInfoDTO.SparkConf);

        ArrayList<String> sshKeyList = new ArrayList<>();
        if(clusterInfoDTO.SshPublicKeys != null) {
            for (String sshPublicKey : clusterInfoDTO.SshPublicKeys) {
                sshKeyList.add(sshPublicKey);
            }
        }
        SshPublicKeys = Collections.unmodifiableList(sshKeyList);
        DefaultTags = Collections.unmodifiableMap(clusterInfoDTO.DefaultTags);
        CustomTags = Collections.unmodifiableMap(clusterInfoDTO.CustomTags);

        ClusterLogConf = clusterInfoDTO.ClusterLogConf == null
                ? null : new ClusterLogConf(clusterInfoDTO.ClusterLogConf);

        SparkEnvironmentVariables = Collections.unmodifiableMap(clusterInfoDTO.SparkEnvironmentVariables);
    }

    protected BaseCluster(ClustersClient client, ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException, HttpException {
        _client = client;
        _clusterInfoDTO = clusterInfoDTO;

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(clusterInfoDTO);

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
                _clusterInfoDTO = _client.getCluster(getClusterInfo().ClusterId);
                _clusterInfoRequested = true;
                return _clusterInfoDTO;
            } else {
                return _clusterInfoDTO;
            }
        }
    }

    private AwsAttributes initAwsAttributes() throws HttpException {
        if(_clusterInfoDTO.AwsAttributes == null) {
            return new AwsAttributes(getClusterInfo().AwsAttributes);
        } else {
            return new AwsAttributes(_clusterInfoDTO.AwsAttributes);
        }
    }

    private Boolean initElasticDiskEnabled() throws HttpException {
        return getClusterInfo().EnableElasticDisk;
    }

    private Map<String, String> initSparkConf() throws HttpException {
        if(_clusterInfoDTO.SparkConf == null) {
            return getClusterInfo().SparkConf == null
                    ? new HashMap<>() : getClusterInfo().SparkConf;
        } else {
            return _clusterInfoDTO.SparkConf == null ? new HashMap<>() : _clusterInfoDTO.SparkConf;
        }
    }

    private List<String> initSshPublicKeys() throws HttpException {
        List<String> sshPublicKeysList = new ArrayList<>();
        String[] sshPublicKeysDTO;
        if(_clusterInfoDTO.SshPublicKeys == null) {
            sshPublicKeysDTO = getClusterInfo().SshPublicKeys;
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
            Map<String, String> customTagsDTO = getClusterInfo().CustomTags;
            return customTagsDTO == null ? new HashMap<>() : customTagsDTO;
        } else {
            Map<String, String> customTagsDTO =_clusterInfoDTO.CustomTags;
            return customTagsDTO == null ? new HashMap<>() : customTagsDTO;
        }
    }

    private ClusterLogConf initLogConf() throws HttpException {
        if(_clusterInfoDTO.ClusterLogConf == null && _clusterInfoRequested) {
            return null;
        } else if(_clusterInfoDTO.ClusterLogConf == null && !_clusterInfoRequested) {
            if(getClusterInfo().ClusterLogConf == null) {
                return null;
            } else {
                return new ClusterLogConf(getClusterInfo().ClusterLogConf);
            }
        } else {
            return new ClusterLogConf(_clusterInfoDTO.ClusterLogConf);
        }
    }

    private Map<String, String> initSparkEnvironmentVariables() throws HttpException {
        Map<String, String> sparkEvnVarDTO;

        if(_clusterInfoDTO.SparkEnvironmentVariables == null) {
            sparkEvnVarDTO =  getClusterInfo().SparkEnvironmentVariables;
        } else {
            sparkEvnVarDTO =  _clusterInfoDTO.SparkEnvironmentVariables;
        }
        return sparkEvnVarDTO == null ? new HashMap<>() : sparkEvnVarDTO;
    }

    private Map<String, String> initDefaultTags() throws HttpException {
        if(_clusterInfoDTO.DefaultTags == null) {
            return getClusterInfo().DefaultTags;
        } else {
            return _clusterInfoDTO.DefaultTags;
        }
    }



}
