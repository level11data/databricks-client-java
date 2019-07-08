package com.level11data.databricks.instancepool;

import com.level11data.databricks.cluster.AwsAttribute.*;
import com.level11data.databricks.client.entities.clusters.AwsAttributesDTO;

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

    public AwsAttributes(AwsAttributesDTO awsAttributesDTOInfo) {
        FirstOnDemand = awsAttributesDTOInfo.FirstOnDemand;

        String awsAvailability = awsAttributesDTOInfo.Availability;
        if(awsAvailability != null) {
            Availability = AwsAvailability.valueOf(awsAvailability);
        } else {
            Availability = null;
        }

        ZoneId = awsAttributesDTOInfo.ZoneId;
        InstanceProfileARN = awsAttributesDTOInfo.InstanceProfileARN;
        SpotBidPricePercent = awsAttributesDTOInfo.SpotBidPricePercent;

        String ebsVolumeType = awsAttributesDTOInfo.EbsVolumeType;
        if(ebsVolumeType != null) {
            EbsVolumeType = com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType.valueOf(ebsVolumeType);
        } else {
            EbsVolumeType = null;
        }

        EbsVolumeCount = awsAttributesDTOInfo.EbsVolumeCount;
        EbsVolumeSize = awsAttributesDTOInfo.EbsVolumeSize;
    }
}
