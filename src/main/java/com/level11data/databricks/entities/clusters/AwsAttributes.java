package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AwsAttributes {
    public enum EbsVolumeType {
        GENERAL_PURPOSE_SSD, THROUGHPUT_OPTIMIZED_HDD
    }

    public enum AwsAvailability {
        ON_DEMAND, SPOT_WITH_FALLBACK, SPOT
    }

    @JsonProperty("first_on_demand")
    public Integer FirstOnDemand;

    @JsonProperty("availability")
    public AwsAvailability Availability;

    @JsonProperty("zone_id")
    public String ZoneId;

    @JsonProperty("instance_profile_arn")
    public String InstanceProfileARN;

    @JsonProperty("spot_bid_price_percent")
    public Integer SpotBidPricePercent;

    @JsonProperty("ebs_volume_type")
    public EbsVolumeType EbsVolumeType;

    @JsonProperty("ebs_volume_count")
    public Integer EbsVolumeCount;

    @JsonProperty("ebs_volume_size")
    public Integer EbsVolumeSize;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("FirstOnDemand : " + this.FirstOnDemand + ", ");
        stringBuilder.append("Availability : " + this.Availability + ", ");
        stringBuilder.append("ZoneId : " + this.ZoneId + ", ");
        stringBuilder.append("InstanceProfileARN : " + this.InstanceProfileARN + ", ");
        stringBuilder.append("SpotBidPricePercent : " + this.SpotBidPricePercent + ", ");
        stringBuilder.append("EbsVolumeType : " + this.EbsVolumeType + ", ");
        stringBuilder.append("EbsVolumeCount : " + this.EbsVolumeCount + ", ");
        stringBuilder.append("EbsVolumeSize : " + this.EbsVolumeSize + ", ");
        return stringBuilder.toString();
    }

}
