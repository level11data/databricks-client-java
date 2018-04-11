package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.cluster.AwsAttribute.AwsAvailability;
import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;

public interface IClusterBuilder {

    IClusterBuilder withSparkVersion(String sparkVersion);

    IClusterBuilder withNodeType(String nodeTypeId);

    IClusterBuilder withDriverNodeType(String nodeTypeId);

    IClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances);

    IClusterBuilder withAwsAvailability(AwsAvailability availability);

    IClusterBuilder withAwsZone(String zoneId);

    IClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn);

    IClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent);

    IClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                     Integer count,
                                     Integer size);

    IClusterBuilder withElasticDisk();

    IClusterBuilder withSparkConf(String key, String value);

    IClusterBuilder withSshPublicKey(String publicKey);

    IClusterBuilder withCustomTag(String key, String value);

    IClusterBuilder withDbfsLogConf(String destination);

    IClusterBuilder withS3LogConf(String destination,
                                  String region,
                                  String endpoint);

    IClusterBuilder withS3LogConfEncryption(String encryptionType,
                                            String kmsKey,
                                            String cannedAcl);

    IClusterBuilder withSparkEnvironmentVariable(String key, String value);

    ClusterSpec createClusterSpec() throws ClusterConfigException;
}