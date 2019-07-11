package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.cluster.AwsAttribute.AwsAvailability;
import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.job.builder.AbstractAutomatedJobBuilder;

public class AutomatedClusterBuilder extends AbstractClusterBuilder implements ClusterBuilder {
    private AbstractAutomatedJobBuilder _jobBuilder;

    public AutomatedClusterBuilder(ClustersClient client, String clusterName, Integer numWorkers) {
        super(client, clusterName, numWorkers);
    }

    public AutomatedClusterBuilder(ClustersClient client, Integer numWorkers) {
        this(client, (String) null, numWorkers);
    }

    public AutomatedClusterBuilder(ClustersClient client, String clusterName, Integer minWorkers, Integer maxWorkers) {
        super(client, clusterName, minWorkers, maxWorkers);
    }

    public AutomatedClusterBuilder(ClustersClient client, Integer minWorkers, Integer maxWorkers) {
        this(client, null, minWorkers, maxWorkers);
    }

    @Override
    protected ClusterInfoDTO applySettings(ClusterInfoDTO clusterInfoDTO) {
        clusterInfoDTO = super.applySettings(clusterInfoDTO);

        //nothing specific to AutomatedCluster
        return clusterInfoDTO;
    }

    @Override
    protected void validateBuilder(boolean clusterNameRequired) throws ClusterConfigException {
        super.validateBuilder(clusterNameRequired);
    }

    private void validateBuilder(ClusterInfoDTO clusterInfoDTO) throws ClusterConfigException {
        validateBuilder(false);

        if(clusterInfoDTO.AutoTerminationMinutes != null) {
            throw new ClusterConfigException("AutomatedCluster ClusterInfoDTO Cannot Have AutoTerminationMinutes set");
        }
    }

    @Override
    public AutomatedClusterBuilder withName(String clusterName) {
        return (AutomatedClusterBuilder)super.withName(clusterName);
    }

    @Override
    public AutomatedClusterBuilder withSparkVersion(String sparkVersion) {
        return (AutomatedClusterBuilder)super.withSparkVersion(sparkVersion);
    }

    @Override
    public AutomatedClusterBuilder withNodeType(String nodeTypeId) {
        return (AutomatedClusterBuilder)super.withNodeType(nodeTypeId);
    }

    @Override
    public AutomatedClusterBuilder withDriverNodeType(String nodeTypeId) {
        return (AutomatedClusterBuilder)super.withDriverNodeType(nodeTypeId);
    }

    @Override
    public AutomatedClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances) {
        return (AutomatedClusterBuilder)super.withAwsFirstOnDemand(onDemandInstances);
    }

    @Override
    public AutomatedClusterBuilder withAwsAvailability(AwsAvailability availability) {
        return (AutomatedClusterBuilder)super.withAwsAvailability(availability);
    }

    @Override
    public AutomatedClusterBuilder withAwsZone(String zoneId) {
        return (AutomatedClusterBuilder)super.withAwsZone(zoneId);
    }

    @Override
    public AutomatedClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        return (AutomatedClusterBuilder)super.withAwsInstanceProfileArn(instanceProfileArn);
    }

    @Override
    public AutomatedClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        return (AutomatedClusterBuilder)super.withAwsSpotBidPricePercent(spotBidPricePercent);
    }

    @Override
    public AutomatedClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                                      Integer count,
                                                      Integer size) {
        return (AutomatedClusterBuilder)super.withAwsEbsVolume(type, count, size);
    }

    @Override
    public AutomatedClusterBuilder withElasticDisk() {
        return (AutomatedClusterBuilder)super.withElasticDisk();
    }

    @Override
    public AutomatedClusterBuilder withSparkConf(String key, String value){
        return (AutomatedClusterBuilder)super.withSparkConf(key, value);
    }

    @Override
    public AutomatedClusterBuilder withSshPublicKey(String publicKey){
        return (AutomatedClusterBuilder)super.withSshPublicKey(publicKey);
    }

    @Override
    public AutomatedClusterBuilder withCustomTag(String key, String value) {
        return (AutomatedClusterBuilder)super.withCustomTag(key, value);
    }

    @Override
    public AutomatedClusterBuilder withDbfsLogConf(String destination) {
        return (AutomatedClusterBuilder)super.withDbfsLogConf(destination);
    }

    @Override
    public AutomatedClusterBuilder withS3LogConf(String destination,
                                                   String region,
                                                   String endpoint) {
        return (AutomatedClusterBuilder) super.withS3LogConf(destination, region, endpoint);
    }

    @Override
    public AutomatedClusterBuilder withS3LogConfEncryption(String encryptionType,
                                                             String kmsKey,
                                                             String cannedAcl) {
        return (AutomatedClusterBuilder)super.withS3LogConfEncryption(encryptionType, kmsKey, cannedAcl);
    }

    @Override
    public AutomatedClusterBuilder withSparkEnvironmentVariable(String key, String value) {
        return (AutomatedClusterBuilder)super.withSparkEnvironmentVariable(key, value);
    }

    @Override
    public AutomatedClusterBuilder withInstancePool(InstancePool instancePool) {
        return (AutomatedClusterBuilder)super.withInstancePool(instancePool);
    }

    public ClusterSpec createClusterSpec() throws ClusterConfigException {
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO = applySettings(clusterInfoDTO);
        validateBuilder(clusterInfoDTO);
        return new ClusterSpec(clusterInfoDTO);
    }

}
