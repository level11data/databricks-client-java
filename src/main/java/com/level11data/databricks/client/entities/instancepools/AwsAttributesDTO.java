package com.level11data.databricks.client.entities.instancepools;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AwsAttributesDTO {
    @JsonProperty("first_on_demand")
    public Integer FirstOnDemand;

    @JsonProperty("availability")
    public String Availability;

    @JsonProperty("zone_id")
    public String ZoneId;

    @JsonProperty("instance_profile_arn")
    public String InstanceProfileARN;

    @JsonProperty("spot_bid_price_percent")
    public Integer SpotBidPricePercent;

    @Override
    public String toString() {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            return ow.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Could Not Marshal Object to JSON";
        }
    }
}
