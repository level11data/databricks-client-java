package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkNode {
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
    public SparkNodeAwsAttributes NodeAwsAttributes;

    @JsonProperty("host_private_ip")
    public String HostPrivateIP;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("PrivateIp : " + this.PrivateIP + ", ");
        stringBuilder.append("PublicDNS : " + this.PublicDNS + '\n');
        stringBuilder.append("NodeId : " + this.NodeId + '\n');
        stringBuilder.append("InstanceId : " + this.InstanceId + '\n');
        stringBuilder.append("StartTimestamp : " + this.StartTimestamp + '\n');
        stringBuilder.append("NodeAwsAttributes : " + this.NodeAwsAttributes + '\n');
        stringBuilder.append("HostPrivateIP : " + this.HostPrivateIP + '\n');
        return stringBuilder.toString();
    }

}
