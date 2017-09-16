package com.level11data.databricks.cluster;

import java.util.Map;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.cluster.AwsAttribute.*;
import com.level11data.databricks.entities.clusters.*;
import com.level11data.databricks.entities.clusters.AwsAttributesDTO;
import com.level11data.databricks.entities.clusters.AutoScaleDTO;
import com.level11data.databricks.entities.clusters.S3StorageInfoDTO;
import com.level11data.databricks.entities.clusters.DbfsStorageInfoDTO;
import com.level11data.databricks.entities.clusters.ClusterLogConfDTO;

public class ClusterBuilder {
    private ClustersClient _client;
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
    private Integer _autoTerminationMinutes;
    private Boolean _enableElasticDisk = false;
    private Map<String, String> _sparkConf;
    private String[] _sshPublicKeys;
    private Map<String, String> _customTags;
    private String _logConfDbfsDestination;
    private String _logConfS3Destination;
    private String _logConfS3Region;
    private String _logConfS3Endpoint;
    private Boolean _logConfS3EnableEncryption = false;
    private String _logConfS3EncryptionType;
    private String _logConfS3KmsKey;
    private String _logConfS3CannedAcl;
    private Map<String, String> _sparkEnvironmentVariables;

    public ClusterBuilder(ClustersClient client, String clusterName, Integer numWorkers) {
        _client = client;
        _clusterName = clusterName;
        _numWorkers = numWorkers;
    }

    public ClusterBuilder(ClustersClient client, String clusterName, Integer minWorkers, Integer maxWorkers) {
        _client = client;
        _clusterName = clusterName;
        _autoscaleMinWorkers = minWorkers;
        _autoscaleMaxWorkers = maxWorkers;
    }

    public ClusterBuilder withSparkVersion(String sparkVersion) {
        _sparkVersion = sparkVersion;
        return this;
    }

    public ClusterBuilder withNodeType(String nodeTypeId) {
        _nodeType = nodeTypeId;
        return this;
    }

    public ClusterBuilder withDriverNodeType(String nodeTypeId) {
        _driverNodeType = nodeTypeId;
        return this;
    }

    public ClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances) {
        _awsFirstOnDemand = onDemandInstances;
        return this;
    }

    public ClusterBuilder withAwsAvailability(AwsAvailability availability) {
        _awsAvailability = availability;
        return this;
    }

    public ClusterBuilder withAwsZone(String zoneId) {
        _awsZone = zoneId;
        return this;
    }

    public ClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        _awsInstanceProfileArn = instanceProfileArn;
        return this;
    }

    public ClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        _awsSpotBidPricePercent = spotBidPricePercent;
        return this;
    }

    public ClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                           Integer count,
                                           Integer size) {
        _awsEbsVolumeType = type;
        _awsEbsVolumeCount = count;
        _awsEbsVolumeSize = size;
        return this;
    }

    public ClusterBuilder withAutoTerminationMinutes(Integer minutes) {
        _autoTerminationMinutes = minutes;
        return this;
    }

    public ClusterBuilder withElasticDisk() {
        _enableElasticDisk = true;
        return this;
    }

    public ClusterBuilder withSparkConf(String key, String value){
        _sparkConf.put(key, value);
        return this;
    }

    public ClusterBuilder withSshPublicKey(String publicKey){
        //TODO Create List and add value
        return this;
    }

    public ClusterBuilder withCustomTag(String key, String value) {
        _customTags.put(key, value);
        return this;
    }

    public ClusterBuilder withDbfsLogConf(String destination) {
        _logConfDbfsDestination = destination;
        return this;
    }

    public ClusterBuilder withS3LogConf(String destination,
                                        String region,
                                        String endpoint) {
        _logConfS3Destination = destination;
        _logConfS3Region = region;
        _logConfS3Endpoint = endpoint;
        return this;
    }

    public ClusterBuilder withS3LogConfEncryption(String encryptionType,
                                        String kmsKey,
                                        String cannedAcl) {
        _logConfS3EnableEncryption = true;
        _logConfS3EncryptionType = encryptionType;
        _logConfS3KmsKey = kmsKey;
        _logConfS3CannedAcl = cannedAcl;  //TODO check if cannedAcl is part of encryption or its own thing
        return this;
    }

    public ClusterBuilder withSparkEnvironmentVariable(String key, String value) {
        _sparkEnvironmentVariables.put(key, value);
        return this;
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

    public Cluster create() throws ClusterConfigException, HttpException {
        validateLogConf();

        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();

        if(_autoscaleMinWorkers != null && _autoscaleMaxWorkers != null) {
            AutoScaleDTO autoScaleDTO = new AutoScaleDTO();
            autoScaleDTO.MinWorkers = _autoscaleMinWorkers;
            autoScaleDTO.MaxWorkers = _autoscaleMaxWorkers;
            clusterInfoDTO.AutoScale = autoScaleDTO;
        }

        clusterInfoDTO.AutoTerminationMinutes = _autoTerminationMinutes;

        if(_awsAvailability != null ||
           _awsEbsVolumeCount != null ||
           _awsEbsVolumeSize != null ||
           _awsEbsVolumeType != null ||
           _awsFirstOnDemand != null ||
           _awsInstanceProfileArn != null ||
           _awsSpotBidPricePercent != null ||
           _awsZone != null) {
            AwsAttributesDTO awsAttr = new AwsAttributesDTO();
            awsAttr.Availability = _awsAvailability.toString();
            awsAttr.EbsVolumeCount = _awsEbsVolumeCount;
            awsAttr.EbsVolumeSize = _awsEbsVolumeSize;
            awsAttr.EbsVolumeType = _awsEbsVolumeType.toString();
            awsAttr.FirstOnDemand = _awsFirstOnDemand;
            awsAttr.InstanceProfileARN = _awsInstanceProfileArn;
            awsAttr.SpotBidPricePercent = _awsSpotBidPricePercent;
            awsAttr.ZoneId = _awsZone;
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

        clusterInfoDTO.ClusterName = _clusterName;
        clusterInfoDTO.CustomTags = _customTags;
        clusterInfoDTO.DriverNodeTypeId = _driverNodeType;
        clusterInfoDTO.EnableElasticDisk = _enableElasticDisk;
        clusterInfoDTO.NodeTypeId = _nodeType;
        clusterInfoDTO.NumWorkers = _numWorkers;
        clusterInfoDTO.SparkConf = _sparkConf;
        clusterInfoDTO.SparkEnvironmentVariables = _sparkEnvironmentVariables;
        clusterInfoDTO.SparkVersion = _sparkVersion;
        clusterInfoDTO.SshPublicKeys = _sshPublicKeys;

        //create cluster via client
        clusterInfoDTO.ClusterId = _client.create(clusterInfoDTO);

        return new Cluster(_client, clusterInfoDTO);
    }
}
