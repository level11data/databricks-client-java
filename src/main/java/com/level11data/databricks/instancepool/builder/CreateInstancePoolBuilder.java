package com.level11data.databricks.instancepool.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.InstancePoolsClient;
import com.level11data.databricks.client.entities.instancepools.AwsAttributesDTO;
import com.level11data.databricks.client.entities.instancepools.DiskSpecDTO;
import com.level11data.databricks.client.entities.instancepools.DiskTypeDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolInfoDTO;
import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.instancepool.AwsAvailability;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.InstancePoolConfigException;

import java.util.ArrayList;
import java.util.Map;

public class CreateInstancePoolBuilder extends AbstractInstancePoolBuilder {

    private final InstancePoolsClient _client;
    private String _nodeTypeId;
    private Map<String,String> _customTags;
    private ArrayList<String> _preloadedSparkVersions = new ArrayList<String>();
    private ArrayList<String> _preloadedDockerImages = new ArrayList<String>();
    private AwsAvailability _awsAvailability;
    private String _awsZoneId;
    private String _awsInstanceProfileArn;
    private Integer _awsSpotBidPricePercent;
    private EbsVolumeType _awsEbsVolumeType;
    private Integer _diskCount;
    private Integer _diskSize;
    private boolean _enableElasticDisk;

    public CreateInstancePoolBuilder(InstancePoolsClient client) {
        super(client);
        _client = client;
    }

    public CreateInstancePoolBuilder withName(String instancePoolName) {
        return (CreateInstancePoolBuilder)super.withName(instancePoolName);
    }

    public CreateInstancePoolBuilder withMinIdleInstances(int minIdleInstances) {
        return (CreateInstancePoolBuilder) super.withMinIdleInstances(minIdleInstances);
    }

    public CreateInstancePoolBuilder withMaxCapacity(int maxCapacity) {
        return (CreateInstancePoolBuilder) super.withMaxCapacity(maxCapacity);
    }

    public CreateInstancePoolBuilder withIdleInstanceAutoTerminationMinutes(int idleInstanceAutoTerminationMinutes) {
        return (CreateInstancePoolBuilder) super.withIdleInstanceAutoTerminationMinutes(idleInstanceAutoTerminationMinutes);
    }

    public CreateInstancePoolBuilder withNodeTypeId(String nodeTypeId) {
        _nodeTypeId = nodeTypeId;
        return this;
    }

    public CreateInstancePoolBuilder withCustomTags(Map<String,String> customTags) {
        _customTags = customTags;
        return this;
    }

    public CreateInstancePoolBuilder withPreloadedSparkVersion(String sparkVersion) {
        _preloadedSparkVersions.add(sparkVersion);
        return this;
    }

    public CreateInstancePoolBuilder withPreloadedDockerImage(String dockerImage) {
        _preloadedDockerImages.add(dockerImage);
        return this;
    }

    public CreateInstancePoolBuilder withElasticDisk() {
        _enableElasticDisk = true;
        return this;
    }

    public CreateInstancePoolBuilder withAwsAvailability(AwsAvailability availability) {
        _awsAvailability = availability;
        return this;
    }

    public CreateInstancePoolBuilder withAwsZone(String zoneId) {
        _awsZoneId = zoneId;
        return this;
    }

    public CreateInstancePoolBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        _awsInstanceProfileArn = instanceProfileArn;
        return this;
    }

    public CreateInstancePoolBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        _awsSpotBidPricePercent = spotBidPricePercent;
        return this;
    }

    public CreateInstancePoolBuilder withDiscSpec(EbsVolumeType type, int count, int size) {
        _awsEbsVolumeType = type;
        _diskCount = count;
        _diskSize = size;
        return this;
    }

    private void validate() throws InstancePoolConfigException {
        if(_nodeTypeId == null ||  _nodeTypeId.isEmpty()) {
            throw new InstancePoolConfigException("InstancePool requires NodeType");
        }
    }

    public InstancePool create() throws InstancePoolConfigException {
        validate();

        //create DTO
        InstancePoolInfoDTO infoDTO = new InstancePoolInfoDTO();
        infoDTO = applySettings(infoDTO);

        infoDTO.NodeTypeId = _nodeTypeId;
        infoDTO.CustomTags = _customTags;
        infoDTO.PreloadedSparkVersions = _preloadedSparkVersions.toArray(new String[_preloadedSparkVersions.size()]);
        infoDTO.PreloadedDockerImages = _preloadedDockerImages.toArray(new String[_preloadedDockerImages.size()]);
        infoDTO.EnableElasticDisk = _enableElasticDisk;

        if(_awsAvailability != null ||
                _diskCount != null ||
                _diskSize != null ||
                _awsEbsVolumeType != null ||
                _awsInstanceProfileArn != null ||
                _awsSpotBidPricePercent != null ||
                _awsZoneId != null) {
            AwsAttributesDTO awsAttrDTO = new AwsAttributesDTO();

            if(_awsAvailability != null) {
                awsAttrDTO.Availability = _awsAvailability.toString();
            }

            if(_diskCount != null ||
                    _diskSize != null ||
               _awsEbsVolumeType != null ) {
                DiskSpecDTO diskSpecDTO = new DiskSpecDTO();
                diskSpecDTO.DiskCount = _diskCount.intValue();
                diskSpecDTO.DiskSize = _diskSize.intValue();

                if(_awsEbsVolumeType != null) {
                    DiskTypeDTO diskTypeDTO = new DiskTypeDTO();
                    diskTypeDTO.EbsVolumeType = _awsEbsVolumeType.toString();
                    diskSpecDTO.DiskType = diskTypeDTO;
                }
                infoDTO.DiskSpec = diskSpecDTO;
            }
            awsAttrDTO.InstanceProfileARN = _awsInstanceProfileArn;
            awsAttrDTO.SpotBidPricePercent = _awsSpotBidPricePercent;
            awsAttrDTO.ZoneId = _awsZoneId;
            infoDTO.AwsAttributes = awsAttrDTO;
        }

        //create InstancePool via client
        try {
            String instancePoolId = _client.createInstancePool(infoDTO);
            return new InstancePool(_client, infoDTO, instancePoolId);
        } catch(HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

}
