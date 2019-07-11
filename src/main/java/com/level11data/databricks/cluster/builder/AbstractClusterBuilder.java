package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.cluster.AwsAttribute.*;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.client.entities.clusters.*;
import com.level11data.databricks.instancepool.InstancePool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

abstract public class AbstractClusterBuilder {
    protected ClustersClient _client;

    private String _clusterName;
    private Integer _numWorkers;
    private Integer _autoscaleMinWorkers;
    private Integer _autoscaleMaxWorkers;
    private String _sparkVersion;
    private String _nodeType;
    private String _driverNodeType;
    private Integer _awsFirstOnDemand;
    private AwsAvailability _awsAvailability;
    private String _awsZone;
    private String _awsInstanceProfileArn;
    private Integer _awsSpotBidPricePercent;
    private EbsVolumeType _awsEbsVolumeType;
    private Integer _awsEbsVolumeCount;
    private Integer _awsEbsVolumeSize;
    private Boolean _enableElasticDisk = false;
    private Map<String, String> _sparkConf = new HashMap<String, String>();
    private ArrayList<String> _sshPublicKeys = new ArrayList<String>();
    private Map<String, String> _customTags = new HashMap<String, String>();;
    private String _logConfDbfsDestination;
    private String _logConfS3Destination;
    private String _logConfS3Region;
    private String _logConfS3Endpoint;
    private Boolean _logConfS3EnableEncryption = false;
    private String _logConfS3EncryptionType;
    private String _logConfS3KmsKey;
    private String _logConfS3CannedAcl;
    private Map<String, String> _sparkEnvironmentVariables = new HashMap<String, String>();
    private InstancePool _instancePool;

    //autoscaling; with Name
    public AbstractClusterBuilder(ClustersClient client, String clusterName, Integer minWorkers, Integer maxWorkers) {
        _client = client;
        _clusterName = clusterName;
        _autoscaleMinWorkers = minWorkers;
        _autoscaleMaxWorkers = maxWorkers;
    }

    //fixed; with Name
    public AbstractClusterBuilder(ClustersClient client, String clusterName, Integer numWorkers) {
        _client = client;
        _clusterName = clusterName;
        _numWorkers = numWorkers;
    }

    //fixed; No Name
    public AbstractClusterBuilder(ClustersClient client, Integer numWorkers) {
        this(client, (String)null, numWorkers);
    }

    //autoscaling; No Name
    public AbstractClusterBuilder(ClustersClient client, Integer minWorkers, Integer maxWorkers) {
        this(client, null, minWorkers, maxWorkers);
    }

    protected AbstractClusterBuilder withName(String clusterName) {
        _clusterName = clusterName;
        return this;
    }

    protected AbstractClusterBuilder withSparkVersion(String sparkVersion) {
        _sparkVersion = sparkVersion;
        return this;
    }

    protected AbstractClusterBuilder withNodeType(String nodeTypeId) {
        _nodeType = nodeTypeId;
        return this;
    }

    protected AbstractClusterBuilder withDriverNodeType(String nodeTypeId) {
        _driverNodeType = nodeTypeId;
        return this;
    }

    protected AbstractClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances) {
        _awsFirstOnDemand = onDemandInstances;
        return this;
    }

    protected AbstractClusterBuilder withAwsAvailability(AwsAvailability availability) {
        _awsAvailability = availability;
        return this;
    }

    protected AbstractClusterBuilder withAwsZone(String zoneId) {
        _awsZone = zoneId;
        return this;
    }

    protected AbstractClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        _awsInstanceProfileArn = instanceProfileArn;
        return this;
    }

    protected AbstractClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        _awsSpotBidPricePercent = spotBidPricePercent;
        return this;
    }

    protected AbstractClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                           Integer count,
                                           Integer size) {
        _awsEbsVolumeType = type;
        _awsEbsVolumeCount = count;
        _awsEbsVolumeSize = size;
        return this;
    }

    protected AbstractClusterBuilder withElasticDisk() {
        _enableElasticDisk = true;
        return this;
    }

    protected AbstractClusterBuilder withSparkConf(String key, String value){
        _sparkConf.put(key, value);
        return this;
    }

    protected AbstractClusterBuilder withSshPublicKey(String publicKey){
        _sshPublicKeys.add(publicKey);
        return this;
    }

    protected AbstractClusterBuilder withCustomTag(String key, String value) {
        _customTags.put(key, value);
        return this;
    }

    protected AbstractClusterBuilder withDbfsLogConf(String destination) {
        _logConfDbfsDestination = destination;
        return this;
    }

    protected AbstractClusterBuilder withS3LogConf(String destination,
                                                   String region,
                                                   String endpoint) {
        _logConfS3Destination = destination;
        _logConfS3Region = region;
        _logConfS3Endpoint = endpoint;
        return this;
    }

    protected AbstractClusterBuilder withS3LogConfEncryption(String encryptionType,
                                                             String kmsKey,
                                                             String cannedAcl) {
        _logConfS3EnableEncryption = true;
        _logConfS3EncryptionType = encryptionType;
        _logConfS3KmsKey = kmsKey;
        _logConfS3CannedAcl = cannedAcl;  //TODO check if cannedAcl is part of encryption or its own thing
        return this;
    }

    protected AbstractClusterBuilder withSparkEnvironmentVariable(String key, String value) {
        _sparkEnvironmentVariables.put(key, value);
        return this;
    }

    protected AbstractClusterBuilder withInstancePool(InstancePool instancePool){
        _instancePool = instancePool;
        return this;
    };

    protected void validateBuilder(boolean clusterNameRequired) throws ClusterConfigException {
        validateLogConf();

        if(clusterNameRequired) {
            if(_clusterName == null) {
                throw new ClusterConfigException("AbstractCluster Name is Required");
            }
        }
    }

    private void validateLogConf() throws ClusterConfigException {
        //check that either s3 or dbfs log conf is set (but not both)
        //It is permissible for no log configuration to be set
        if(_logConfDbfsDestination != null && _logConfS3Destination !=null){
            throw new ClusterConfigException(
                    "Both DBFS Log Configuration AND S3 Log Configuration are set; choose either DBFS or S3");
        }

        //TODO check that if s3 log confEncryption, that destination, region, and endpoint are all set
        if(_logConfS3EnableEncryption) {
            if(_logConfS3EncryptionType == null) {
                throw new ClusterConfigException("S3 Log Configuration Encryption Enabled, but no Encryption Type Specified");
            } else if(_logConfS3KmsKey == null) {
                throw new ClusterConfigException("S3 Log Configuration Encryption Enabled, but no KMS Key Specified");
            } else if(_logConfS3CannedAcl == null) {
                //TODO Not sure about this one; if it is an error or not
                throw new ClusterConfigException("S3 Log Configuration Encryption Enabled, but no Canned ACL Specified");
            }
        }
    }

    private boolean anyAwsAttributesPopulated() {
        return _awsAvailability != null ||
                _awsEbsVolumeCount != null ||
                _awsEbsVolumeSize != null ||
                _awsEbsVolumeType != null ||
                _awsFirstOnDemand != null ||
                _awsInstanceProfileArn != null ||
                _awsSpotBidPricePercent != null ||
                _awsZone != null;
    }

    protected ClusterInfoDTO applySettings(ClusterInfoDTO clusterInfoDTO) {
        clusterInfoDTO.ClusterName = _clusterName;
        clusterInfoDTO.NumWorkers = _numWorkers;

        if(_autoscaleMinWorkers != null && _autoscaleMaxWorkers != null) {
            AutoScaleDTO autoScaleDTO = new AutoScaleDTO();
            autoScaleDTO.MinWorkers = _autoscaleMinWorkers;
            autoScaleDTO.MaxWorkers = _autoscaleMaxWorkers;
            clusterInfoDTO.AutoScale = autoScaleDTO;
        }

        //cannot specify AWS Attributes if an InstancePoolId is supplied
        //TODO add WARN logging statement if values are being overridden
        if(anyAwsAttributesPopulated() && _instancePool == null) {
            AwsAttributesDTO awsAttr = new AwsAttributesDTO();

            if (_awsAvailability != null) {
                awsAttr.Availability = _awsAvailability.toString();
            }

            awsAttr.EbsVolumeCount = _awsEbsVolumeCount;
            awsAttr.EbsVolumeSize = _awsEbsVolumeSize;
            if (_awsEbsVolumeType != null) {
                awsAttr.EbsVolumeType = _awsEbsVolumeType.toString();
            }
            awsAttr.ZoneId = _awsZone;
            awsAttr.InstanceProfileARN = _awsInstanceProfileArn;
            awsAttr.SpotBidPricePercent = _awsSpotBidPricePercent;
            awsAttr.FirstOnDemand = _awsFirstOnDemand;
            clusterInfoDTO.AwsAttributes = awsAttr;
        }

        if(_logConfDbfsDestination != null || _logConfS3Destination != null) {
            ClusterLogConfDTO logConf = new ClusterLogConfDTO();

            if(_logConfDbfsDestination != null){

                DbfsStorageInfoDTO dbfsLogConf = new DbfsStorageInfoDTO();
                dbfsLogConf.Destination = _logConfDbfsDestination;
                logConf.DBFS = dbfsLogConf;
            } else {
                S3StorageInfoDTO s3LogConf = new S3StorageInfoDTO();
                s3LogConf.CannedAcl = _logConfS3CannedAcl;
                s3LogConf.Destination = _logConfS3Destination;
                s3LogConf.EnableEncryption = _logConfS3EnableEncryption;
                s3LogConf.EncryptionType = _logConfS3EncryptionType;
                s3LogConf.Endpoint = _logConfS3Endpoint;
                s3LogConf.KmsKey = _logConfS3KmsKey;
                s3LogConf.Region = _logConfS3Region;
                logConf.S3 = s3LogConf;
            }
            clusterInfoDTO.ClusterLogConf = logConf;
        }

        clusterInfoDTO.CustomTags = _customTags;
        clusterInfoDTO.DriverNodeTypeId = _driverNodeType;

        //cannot specify some properties if an InstancePoolId is supplied
        if(_instancePool == null) {
            //TODO add WARN logging statement if values are being overridden
            clusterInfoDTO.NodeTypeId = _nodeType;
            clusterInfoDTO.EnableElasticDisk = _enableElasticDisk;
        }

        clusterInfoDTO.SparkConf = _sparkConf;
        clusterInfoDTO.SparkEnvironmentVariables = _sparkEnvironmentVariables;
        clusterInfoDTO.SparkVersionKey = _sparkVersion;
        if(_sshPublicKeys.size() > 0) {
            clusterInfoDTO.SshPublicKeys = _sshPublicKeys.toArray(new String[_sshPublicKeys.size()]);
        }

        if(_instancePool != null) {
            clusterInfoDTO.InstancePoolId = _instancePool.getId();
        }
        return clusterInfoDTO;
    }
}
