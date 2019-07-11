package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.cluster.AwsAttribute.AwsAvailability;
import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.library.*;

import java.util.ArrayList;

public class InteractiveClusterBuilder extends AbstractClusterBuilder implements ClusterBuilder {
    protected ClustersClient _client;

    private Integer _autoTerminationMinutes;
    private ArrayList<AbstractLibrary> _libraries = new ArrayList<>();

    public InteractiveClusterBuilder(ClustersClient client, String clusterName, Integer numWorkers) {
        super(client, clusterName, numWorkers);
        _client = client;
    }

    public InteractiveClusterBuilder(ClustersClient client, String clusterName, Integer minWorkers, Integer maxWorkers) {
        super(client, clusterName, minWorkers, maxWorkers);
        _client = client;
    }

    @Override
    protected void validateBuilder(boolean clusterNameRequired) throws ClusterConfigException {
        super.validateBuilder(clusterNameRequired);
    }

    private void validateBuilder() throws ClusterConfigException {
        validateBuilder(true);
    }

    @Override
    protected ClusterInfoDTO applySettings(ClusterInfoDTO clusterInfoDTO) {
        clusterInfoDTO = super.applySettings(clusterInfoDTO);

        //specific to InteractiveCluster
        clusterInfoDTO.AutoTerminationMinutes = _autoTerminationMinutes;

        return clusterInfoDTO;
    }

    @Override
    public InteractiveClusterBuilder withName(String clusterName) {
        return (InteractiveClusterBuilder)super.withName(clusterName);
    }

    @Override
    public InteractiveClusterBuilder withSparkVersion(String sparkVersion) {
        return (InteractiveClusterBuilder)super.withSparkVersion(sparkVersion);
    }

    @Override
    public InteractiveClusterBuilder withNodeType(String nodeTypeId) {
        return (InteractiveClusterBuilder)super.withNodeType(nodeTypeId);
    }

    @Override
    public InteractiveClusterBuilder withDriverNodeType(String nodeTypeId) {
        return (InteractiveClusterBuilder)super.withDriverNodeType(nodeTypeId);
    }

    @Override
    public InteractiveClusterBuilder withAwsFirstOnDemand(Integer onDemandInstances) {
        return (InteractiveClusterBuilder)super.withAwsFirstOnDemand(onDemandInstances);
    }

    @Override
    public InteractiveClusterBuilder withAwsAvailability(AwsAvailability availability) {
        return (InteractiveClusterBuilder)super.withAwsAvailability(availability);
    }

    @Override
    public InteractiveClusterBuilder withAwsZone(String zoneId) {
        return (InteractiveClusterBuilder)super.withAwsZone(zoneId);
    }

    @Override
    public InteractiveClusterBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        return (InteractiveClusterBuilder)super.withAwsInstanceProfileArn(instanceProfileArn);
    }

    @Override
    public InteractiveClusterBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        return (InteractiveClusterBuilder)super.withAwsSpotBidPricePercent(spotBidPricePercent);
    }

    @Override
    public InteractiveClusterBuilder withAwsEbsVolume(EbsVolumeType type,
                                              Integer count,
                                              Integer size) {
        return (InteractiveClusterBuilder)super.withAwsEbsVolume(type, count, size);
    }

    @Override
    public InteractiveClusterBuilder withElasticDisk() {
        return (InteractiveClusterBuilder)super.withElasticDisk();
    }

    @Override
    public InteractiveClusterBuilder withSparkConf(String key, String value){
        return (InteractiveClusterBuilder)super.withSparkConf(key, value);
    }

    @Override
    public InteractiveClusterBuilder withSshPublicKey(String publicKey){
        return (InteractiveClusterBuilder)super.withSshPublicKey(publicKey);
    }

    @Override
    public InteractiveClusterBuilder withCustomTag(String key, String value) {
        return (InteractiveClusterBuilder)super.withCustomTag(key, value);
    }

    @Override
    public InteractiveClusterBuilder withDbfsLogConf(String destination) {
        return (InteractiveClusterBuilder)super.withDbfsLogConf(destination);
    }

    @Override
    public InteractiveClusterBuilder withS3LogConf(String destination,
                                           String region,
                                           String endpoint) {
        return (InteractiveClusterBuilder) super.withS3LogConf(destination, region, endpoint);
    }

    @Override
    public InteractiveClusterBuilder withS3LogConfEncryption(String encryptionType,
                                                     String kmsKey,
                                                     String cannedAcl) {
        return (InteractiveClusterBuilder)super.withS3LogConfEncryption(encryptionType, kmsKey, cannedAcl);
    }

    @Override
    public InteractiveClusterBuilder withSparkEnvironmentVariable(String key, String value) {
        return (InteractiveClusterBuilder)super.withSparkEnvironmentVariable(key, value);
    }


    public InteractiveClusterBuilder withAutoTerminationMinutes(Integer minutes) {
        _autoTerminationMinutes = minutes;
        return this;
    }

    public InteractiveClusterBuilder withLibrary(AbstractLibrary library) {
        _libraries.add(library);
        return this;
    }

    public InteractiveClusterBuilder withInstancePool(InstancePool instancePool) {
        return (InteractiveClusterBuilder)super.withInstancePool(instancePool);
    }

    public InteractiveCluster create() throws ClusterConfigException {
        validateBuilder();

        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO = applySettings(clusterInfoDTO);

        //create cluster via client
        try {
            clusterInfoDTO.ClusterId = _client.create(clusterInfoDTO);
            InteractiveCluster cluster = new InteractiveCluster(_client, clusterInfoDTO);

            if(_libraries.size() > 0) {
                //TODO include wait step in FUTURE on AbstractLibrary.install
                ClusterState clusterState = cluster.getState();
                if(!clusterState.isFinal()) {
                    while(!cluster.getState().equals(ClusterState.RUNNING)) {
                        try {
                            Thread.sleep(5000); //wait 5 seconds
                        } catch (InterruptedException e){
                            //swallow
                        }
                    }
                } else {
                    throw new ClusterConfigException("AbstractLibrary cannot be attached to cluster because it is "+clusterState.toString());
                }
            }
            //install libraries
            for (AbstractLibrary library : _libraries) {
                if(library instanceof JarLibrary) {
                    cluster.installLibrary((JarLibrary) library);
                } else if (library instanceof EggLibrary) {
                    cluster.installLibrary((EggLibrary) library);
                } else if (library instanceof MavenLibrary) {
                    cluster.installLibrary((MavenLibrary) library);
                } else if (library instanceof PyPiLibrary) {
                    cluster.installLibrary((PyPiLibrary) library);
                } else if (library instanceof CranLibrary) {
                    cluster.installLibrary((CranLibrary) library);
                }
            }
            return cluster;
        } catch (HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public ClusterSpec createClusterSpec() throws ClusterConfigException {
        validateBuilder();
        ClusterInfoDTO clusterInfoDTO = new ClusterInfoDTO();
        clusterInfoDTO = applySettings(clusterInfoDTO);
        return new ClusterSpec(clusterInfoDTO);
    }

}
