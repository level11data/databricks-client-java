package com.level11data.databricks.cluster;


import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.InstancePoolConfigException;

import java.util.*;

public abstract class AbstractBaseCluster {
    private Boolean _clusterInfoRequested = false;
    private ClusterInfoDTO _clusterInfoDTO;
    private ClustersClient _client;
    private String _clusterId;
    protected Boolean IsAutoScaling = false;

    private final Integer _numWorkers;
    private final AutoScale _autoScale;

    private final String _name;
    private final AwsAttributes _awsAttributes;
    private final Boolean _elasticDiskEnabled;
    private final Map<String, String> _sparkConf;
    private final List<String> _sshPublicKeys;
    private final Map<String, String> _defaultTags;
    private final Map<String, String> _customTags;
    private final ClusterLogConf _clusterLogConf;
    private final Map<String, String> _sparkEnvironmentVariables;

    //This signature is used by ClusterSpec
    protected AbstractBaseCluster(ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException {
        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(clusterInfoDTO);

        _clusterInfoDTO = clusterInfoDTO;
        _clusterId = clusterInfoDTO.ClusterId;  //could be null if object represents ClusterSpec

        _name = clusterInfoDTO.ClusterName; //could be null
        _autoScale = initAutoScale(clusterInfoDTO);  //could be null

        _numWorkers = initNumWorkers(clusterInfoDTO);
        _awsAttributes = clusterInfoDTO.AwsAttributes == null ? null : new AwsAttributes(clusterInfoDTO.AwsAttributes);
        _elasticDiskEnabled = clusterInfoDTO.EnableElasticDisk;

        HashMap<String,String> sparkConfMap = new HashMap<>();
        if(clusterInfoDTO.SparkConf != null) {
            sparkConfMap.putAll(clusterInfoDTO.SparkConf);
        }
        _sparkConf = Collections.unmodifiableMap(sparkConfMap);

        ArrayList<String> sshKeyList = new ArrayList<>();
        if(clusterInfoDTO.SshPublicKeys != null) {
            Collections.addAll(sshKeyList, clusterInfoDTO.SshPublicKeys);
        }
        _sshPublicKeys = Collections.unmodifiableList(sshKeyList);

        HashMap<String,String> defaultTagsMap = new HashMap<>();
        if(clusterInfoDTO.DefaultTags != null) {
            defaultTagsMap.putAll(clusterInfoDTO.DefaultTags);
        }
        _defaultTags = Collections.unmodifiableMap(defaultTagsMap);

        HashMap<String,String> customTagsMap = new HashMap<>();
        if(clusterInfoDTO.CustomTags != null) {
            customTagsMap.putAll(clusterInfoDTO.CustomTags);
        }
        _customTags = Collections.unmodifiableMap(customTagsMap);

        _clusterLogConf = clusterInfoDTO.ClusterLogConf == null
                ? null : new ClusterLogConf(clusterInfoDTO.ClusterLogConf);

        HashMap<String,String> sparkEnvVarMap = new HashMap<>();
        if(clusterInfoDTO.SparkEnvironmentVariables != null) {
            sparkEnvVarMap.putAll(clusterInfoDTO.SparkEnvironmentVariables);
        }
        _sparkEnvironmentVariables = Collections.unmodifiableMap(sparkEnvVarMap);
    }

    protected AbstractBaseCluster(ClustersClient client, ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException {
        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(clusterInfoDTO);

        _client = client;
        _clusterInfoDTO = clusterInfoDTO;
        _clusterId = clusterInfoDTO.ClusterId;

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        // therefore they may need to be initialized with an API call to get the ClusterInfo
        _name = initClusterName();
        _numWorkers = initNumWorkers(clusterInfoDTO);
        _autoScale = initAutoScale(clusterInfoDTO);  //could be null

        _awsAttributes = initAwsAttributes();
        _elasticDiskEnabled = initElasticDiskEnabled();
        _sparkConf = Collections.unmodifiableMap(initSparkConf());
        _sshPublicKeys = Collections.unmodifiableList(initSshPublicKeys());
        _defaultTags = Collections.unmodifiableMap(initDefaultTags());
        _customTags = Collections.unmodifiableMap(initCustomTags());
        _sparkEnvironmentVariables = Collections.unmodifiableMap(initSparkEnvironmentVariables());
        _clusterLogConf = initLogConf();

    }

    private AutoScale initAutoScale(ClusterInfoDTO clusterInfoDTO) {
        if(clusterInfoDTO.AutoScale != null){
            IsAutoScaling = true;
            return new AutoScale(clusterInfoDTO.AutoScale);
        } else {
            return null;
        }
    }

    private int initNumWorkers(ClusterInfoDTO clusterInfoDTO) {
        if(clusterInfoDTO.AutoScale != null){
            //if object constructed with ClusterInfo only, then use MinWorkers
            return clusterInfoDTO.AutoScale.MinWorkers;
        } else {
            //cluster size is fixed
            return clusterInfoDTO.NumWorkers;
        }
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(_clusterId != null && info.ClusterId == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have ClusterId");
        }

        if(info.NumWorkers == null && info.AutoScale == null)  {
            throw new ClusterConfigException("ClusterInfoDTO Must Have either NumWorkers OR AutoScaleDTO");
        }
    }

    protected ClusterInfoDTO getClusterInfo() throws ClusterConfigException {
        try {
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
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }

    }

    private String initClusterName() throws ClusterConfigException {
        return getClusterInfo().ClusterName;
    }

    private AwsAttributes initAwsAttributes() throws ClusterConfigException {
        if(getClusterInfo().AwsAttributes == null) {
            return null;
        } else {
            return new AwsAttributes(getClusterInfo().AwsAttributes);
        }
    }



    private Boolean initElasticDiskEnabled() throws ClusterConfigException {
        return getClusterInfo().EnableElasticDisk;
    }

    private Map<String, String> initSparkConf() throws ClusterConfigException {
        if(getClusterInfo().SparkConf == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().SparkConf;
        }
    }

    private List<String> initSshPublicKeys() throws ClusterConfigException {
        if(getClusterInfo().SshPublicKeys == null) {
            return new ArrayList<>();
        } else {
            List<String> sshPublicKeysList = new ArrayList<>();
            Collections.addAll(sshPublicKeysList, getClusterInfo().SshPublicKeys);
            return sshPublicKeysList;
        }
    }

    private Map<String, String> initCustomTags() throws ClusterConfigException {
        if(getClusterInfo().CustomTags == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().CustomTags;
        }
    }

    private ClusterLogConf initLogConf() throws ClusterConfigException {
        if(getClusterInfo().ClusterLogConf == null) {
            return null;
        } else {
            return new ClusterLogConf(getClusterInfo().ClusterLogConf);
        }
    }

    private Map<String, String> initSparkEnvironmentVariables() throws ClusterConfigException {
        if(getClusterInfo().SparkEnvironmentVariables == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().SparkEnvironmentVariables;
        }
    }

    private Map<String, String> initDefaultTags() throws ClusterConfigException {
        if(getClusterInfo().DefaultTags == null) {
            return new HashMap<>();
        } else {
            return getClusterInfo().DefaultTags;
        }
    }

    public int getNumWorkers() throws ClusterConfigException {
        try{
            if(IsAutoScaling && _client != null) {
                //get the number of active executors
                ClusterInfoDTO clusterInfoDTO = _client.getCluster(_clusterId);
                if(clusterInfoDTO.Executors == null) {
                    return 0;
                } else {
                    return clusterInfoDTO.Executors.length;
                }
            } else {
                //either cluster is fixed (not auto-scaling) or there is no client to check
                return _numWorkers;
            }
        }catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public AutoScale getAutoScale(){
        return _autoScale;
    };

    public String getName() {
        return _name;
    };

    public AwsAttributes getAwsAttributes() {
        return _awsAttributes;
    };

    public boolean getElasticDiskEnabled() {
        return _elasticDiskEnabled;
    };

    public Map<String, String> getSparkConf() {
        return _sparkConf;
    };

    public List<String> getSshPublicKeys() {
        return _sshPublicKeys;
    };

    public Map<String, String> getDefaultTags() {
        return _defaultTags;
    };

    public Map<String, String> getCustomTags() {
        return _customTags;
    };

    public ClusterLogConf getClusterLogConf() {
        return _clusterLogConf;
    };

    public Map<String, String> getSparkEnvironmentVariables() {
        return _sparkEnvironmentVariables;
    };
}
