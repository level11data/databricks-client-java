package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.cluster.AwsAttribute.AwsAvailability;
import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.instancepool.InstancePool;

public interface ClusterBuilder {

    ClusterBuilder withName(String clusterName);

    ClusterBuilder withSparkVersion(String sparkVersion);

    ClusterBuilder withNodeType(String nodeTypeId);

    ClusterBuilder withDriverNodeType(String nodeTypeId);

    ClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances);

    ClusterBuilder withAwsAvailability(AwsAvailability availability);

    ClusterBuilder withAwsZone(String zoneId);

    ClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn);

    ClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent);

    ClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                     Integer count,
                                     Integer size);

    ClusterBuilder withElasticDisk();

    ClusterBuilder withSparkConf(String key, String value);

    ClusterBuilder withSshPublicKey(String publicKey);

    ClusterBuilder withCustomTag(String key, String value);

    ClusterBuilder withDbfsLogConf(String destination);

    ClusterBuilder withS3LogConf(String destination,
                                  String region,
                                  String endpoint);

    ClusterBuilder withS3LogConfEncryption(String encryptionType,
                                            String kmsKey,
                                            String cannedAcl);

    ClusterBuilder withSparkEnvironmentVariable(String key, String value);

    ClusterBuilder withInstancePool(InstancePool instancePool);

    ClusterSpec createClusterSpec() throws ClusterConfigException;
}