package com.level11data.databricks.instancepool.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.InstancePoolsClient;
import com.level11data.databricks.client.entities.clusters.AwsAttributesDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolInfoDTO;
import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.instancepool.AwsAvailability;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.InstancePoolConfigException;

import java.util.ArrayList;
import java.util.Map;

public class InstancePoolBuilder {

    private final InstancePoolsClient _client;
    private String _name;
    private String _nodeTypeId;
    private Integer _minIdleInstances;
    private Integer _maxCapacity;
    private Integer _idleInstanceAutoterminationMinutes;
    private Map<String,String> _customTags;
    private ArrayList<String> _preloadedSparkVersions = new ArrayList<String>();
    private ArrayList<String> _preloadedDockerImages = new ArrayList<String>();
    private AwsAvailability _awsAvailability;
    private String _awsZoneId;
    private String _awsInstanceProfileArn;
    private Integer _awsSpotBidPricePercent;
    private EbsVolumeType _awsEbsVolumeType;
    private Integer _awsEbsVolumeCount;
    private Integer _awsEbsVolumeSize;
    private boolean _enableElasticDisk;

    public InstancePoolBuilder(InstancePoolsClient client) {
        _client = client;
    }


    private void validate() throws InstancePoolConfigException {
        if(_name == null ||  _name.isEmpty()) {
            throw new InstancePoolConfigException("InstancePool requires Name");
        }

        if(_nodeTypeId == null ||  _nodeTypeId.isEmpty()) {
            throw new InstancePoolConfigException("InstancePool requires NodeType");
        }

    }

    public InstancePoolBuilder withName(String instancePoolName) {
        _name = instancePoolName;
        return this;
    }

    public InstancePoolBuilder withNodeTypeId(String nodeTypeId) {
        _nodeTypeId = nodeTypeId;
        return this;
    }

    public InstancePoolBuilder withMinIdleInstances(int minIdleInstances) {
        _minIdleInstances = minIdleInstances;
        return this;
    }

    public InstancePoolBuilder withMaxCapacity(int maxCapacity) {
        _maxCapacity = maxCapacity;
        return this;
    }

    public InstancePoolBuilder withIdleInstanceAutoTerminationMinutes(int idleInstanceAutoTerminationMinutes) {
        _idleInstanceAutoterminationMinutes = idleInstanceAutoTerminationMinutes;
        return this;
    }

    public InstancePoolBuilder withCustomTags(Map<String,String> customTags) {
        _customTags = customTags;
        return this;
    }

    public InstancePoolBuilder withPreloadedSparkVersion(String sparkVersion) {
        _preloadedSparkVersions.add(sparkVersion);
        return this;
    }

    public InstancePoolBuilder withPreloadedDockerImage(String dockerImage) {
        _preloadedDockerImages.add(dockerImage);
        return this;
    }

    public InstancePoolBuilder withElasticDisk() {
        _enableElasticDisk = true;
        return this;
    }

    public InstancePoolBuilder withAwsAvailability(AwsAvailability availability) {
        _awsAvailability = availability;
        return this;
    }

    public InstancePoolBuilder withAwsZone(String zoneId) {
        _awsZoneId = zoneId;
        return this;
    }

    public InstancePoolBuilder withAwsInstanceProfileArn(String instanceProfileArn) {
        _awsInstanceProfileArn = instanceProfileArn;
        return this;
    }

    public InstancePoolBuilder withAwsSpotBidPricePercent(Integer spotBidPricePercent) {
        _awsSpotBidPricePercent = spotBidPricePercent;
        return this;
    }

    public InstancePoolBuilder withAwsEbsVolume(EbsVolumeType type, int count, int size) {
        _awsEbsVolumeType = type;
        _awsEbsVolumeCount = count;
        _awsEbsVolumeSize = size;
        return this;
    }

    public InstancePool create() throws InstancePoolConfigException {
        //validate required fields are set
        validate();

        //create DTO
        InstancePoolInfoDTO infoDTO = new InstancePoolInfoDTO();
        infoDTO.InstancePoolName = _name;
        infoDTO.NodeTypeId = _nodeTypeId;
        infoDTO.MinIdleInstances = _minIdleInstances;
        infoDTO.MaxCapacity = _maxCapacity;
        infoDTO.IdleInstanceAutoTerminationMinutes = _idleInstanceAutoterminationMinutes;
        infoDTO.CustomTags = _customTags;
        infoDTO.PreloadedSparkVersions = _preloadedSparkVersions.toArray(new String[_preloadedSparkVersions.size()]);
        infoDTO.PreloadedDockerImages = _preloadedDockerImages.toArray(new String[_preloadedDockerImages.size()]);
        infoDTO.EnableElasticDisk = _enableElasticDisk;

        if(_awsAvailability != null ||
                _awsEbsVolumeCount != null ||
                _awsEbsVolumeSize != null ||
                _awsEbsVolumeType != null ||
                _awsInstanceProfileArn != null ||
                _awsSpotBidPricePercent != null ||
                _awsZoneId != null) {
            AwsAttributesDTO awsAttrDTO = new AwsAttributesDTO();

            if(_awsAvailability != null) {
                awsAttrDTO.Availability = _awsAvailability.toString();
            }
            awsAttrDTO.EbsVolumeCount = _awsEbsVolumeCount;
            awsAttrDTO.EbsVolumeSize = _awsEbsVolumeSize;

            if(_awsEbsVolumeType != null) {
                awsAttrDTO.EbsVolumeType = _awsEbsVolumeType.toString();
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
