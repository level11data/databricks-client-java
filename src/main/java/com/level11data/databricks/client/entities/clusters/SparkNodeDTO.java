package com.level11data.databricks.client.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SparkNodeDTO {
    @JsonProperty("private_ip")
    public String PrivateIP;

    @JsonProperty("public_dns")
    public String PublicDNS;

    @JsonProperty("node_id")
    public String NodeId;

    @JsonProperty("instance_id")
    public String InstanceId;

    @JsonProperty("start_timestamp")
    public BigInteger StartTimestamp;

    @JsonProperty("node_aws_attributes")
    public SparkNodeAwsAttributesDTO NodeAwsAttributes;

    @JsonProperty("host_private_ip")
    public String HostPrivateIP;

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
