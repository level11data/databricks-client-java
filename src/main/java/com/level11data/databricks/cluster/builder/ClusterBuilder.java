package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.cluster.AwsAttribute.*;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.client.entities.clusters.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

abstract public class ClusterBuilder {
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
    private Map<String, String> _sparkEnvironmentVariables = new HashMap<String, String>();;

    protected ClusterBuilder withSparkVersion(String sparkVersion) {
        _sparkVersion = sparkVersion;
        return this;
    }

    protected ClusterBuilder withNodeType(String nodeTypeId) {
        _nodeType = nodeTypeId;
        return this;
    }

    protected ClusterBuilder withDriverNodeType(String nodeTypeId) {
        _driverNodeType = nodeTypeId;
        return this;
    }

    protected ClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances) {
        _awsFirstOnDemand = onDemandInstances;
        return this;
    }

    protected ClusterBuilder withAwsAvailability(AwsAvailability availability) {
        _awsAvailability = availability;
        return this;
    }

    protected ClusterBuilder withAwsZone(String zoneId) {
        _awsZone = zoneId;
        return this;
    }

    protected ClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        _awsInstanceProfileArn = instanceProfileArn;
        return this;
    }

    protected ClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        _awsSpotBidPricePercent = spotBidPricePercent;
        return this;
    }

    protected ClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                           Integer count,
                                           Integer size) {
        _awsEbsVolumeType = type;
        _awsEbsVolumeCount = count;
        _awsEbsVolumeSize = size;
        return this;
    }

    protected ClusterBuilder withElasticDisk() {
        _enableElasticDisk = true;
        return this;
    }

    protected ClusterBuilder withSparkConf(String key, String value){
        _sparkConf.put(key, value);
        return this;
    }

    protected ClusterBuilder withSshPublicKey(String publicKey){
        _sshPublicKeys.add(publicKey);
        return this;
    }

    protected ClusterBuilder withCustomTag(String key, String value) {
        _customTags.put(key, value);
        return this;
    }

    protected ClusterBuilder withDbfsLogConf(String destination) {
        _logConfDbfsDestination = destination;
        return this;
    }

    protected ClusterBuilder withS3LogConf(String destination,
                                                   String region,
                                                   String endpoint) {
        _logConfS3Destination = destination;
        _logConfS3Region = region;
        _logConfS3Endpoint = endpoint;
        return this;
    }

    protected ClusterBuilder withS3LogConfEncryption(String encryptionType,
                                                             String kmsKey,
                                                             String cannedAcl) {
        _logConfS3EnableEncryption = true;
        _logConfS3EncryptionType = encryptionType;
        _logConfS3KmsKey = kmsKey;
        _logConfS3CannedAcl = cannedAcl;  //TODO check if cannedAcl is part of encryption or its own thing
        return this;
    }

    protected ClusterBuilder withSparkEnvironmentVariable(String key, String value) {
        _sparkEnvironmentVariables.put(key, value);
        return this;
    }

    protected void validateLogConf() throws ClusterConfigException {
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

    protected ClusterInfoDTO applySettings(ClusterInfoDTO clusterInfoDTO) {
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

        clusterInfoDTO.CustomTags = _customTags;
        clusterInfoDTO.DriverNodeTypeId = _driverNodeType;
        clusterInfoDTO.EnableElasticDisk = _enableElasticDisk;
        clusterInfoDTO.NodeTypeId = _nodeType;
        clusterInfoDTO.SparkConf = _sparkConf;
        clusterInfoDTO.SparkEnvironmentVariables = _sparkEnvironmentVariables;
        clusterInfoDTO.SparkVersionKey = _sparkVersion;
        if(_sshPublicKeys.size() > 0) {
            clusterInfoDTO.SshPublicKeys = _sshPublicKeys.toArray(new String[_sshPublicKeys.size()]);
        }
        return clusterInfoDTO;
    }

}
