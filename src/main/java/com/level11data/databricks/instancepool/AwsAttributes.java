package com.level11data.databricks.instancepool;

import com.level11data.databricks.client.entities.instancepools.AwsAttributesDTO;

public class AwsAttributes {
    public final AwsAvailability Availability;
    public final String ZoneId;
    public final String InstanceProfileARN;
    public final Integer SpotBidPricePercent;

    public AwsAttributes(AwsAttributesDTO awsAttributesDTOInfo) {
        String awsAvailability = awsAttributesDTOInfo.Availability;
        if(awsAvailability != null) {
            Availability = AwsAvailability.valueOf(awsAvailability);
        } else {
            Availability = null;
        }

        ZoneId = awsAttributesDTOInfo.ZoneId;
        InstanceProfileARN = awsAttributesDTOInfo.InstanceProfileARN;
        SpotBidPricePercent = awsAttributesDTOInfo.SpotBidPricePercent;
    }
}
