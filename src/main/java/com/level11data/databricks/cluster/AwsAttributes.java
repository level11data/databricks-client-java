package com.level11data.databricks.cluster;

import com.level11data.databricks.entities.clusters.AwsAttributes.AwsAvailability;
import com.level11data.databricks.entities.clusters.AwsAttributes.EbsVolumeType;

public class AwsAttributes {
    public final Integer FirstOnDemand;
    public final AwsAvailability Availability;
    public final String ZoneId;
    public final String InstanceProfileARN;
    public final Integer SpotBidPricePercent;
    public final EbsVolumeType EbsVolumeType;
    public final Integer EbsVolumeCount;
    public final Integer EbsVolumeSize;

    public AwsAttributes(Integer firstOnDemand,
            AwsAvailability availability,
            String zoneId,
            String instanceProfileARN,
            Integer spotBidPricePercent,
            EbsVolumeType ebsVolumeType,
            Integer ebsVolumeCount,
            Integer ebsVolumeSize) {
        FirstOnDemand = firstOnDemand;
        Availability = availability;
        ZoneId = zoneId;
        InstanceProfileARN = instanceProfileARN;
        SpotBidPricePercent = spotBidPricePercent;
        EbsVolumeType = ebsVolumeType;
        EbsVolumeCount = ebsVolumeCount;
        EbsVolumeSize = ebsVolumeSize;
    }

    public AwsAttributes(com.level11data.databricks.entities.clusters.AwsAttributes awsAttributesInfo) {
        FirstOnDemand = awsAttributesInfo.FirstOnDemand;
        Availability = awsAttributesInfo.Availability;
        ZoneId = awsAttributesInfo.ZoneId;
        InstanceProfileARN = awsAttributesInfo.InstanceProfileARN;
        SpotBidPricePercent = awsAttributesInfo.SpotBidPricePercent;
        EbsVolumeType = awsAttributesInfo.EbsVolumeType;
        EbsVolumeCount = awsAttributesInfo.EbsVolumeCount;
        EbsVolumeSize = awsAttributesInfo.EbsVolumeSize;
    }
}
