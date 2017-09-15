package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeType {
    @JsonProperty("node_type_id")
    public String Id;

    @JsonProperty("memory_mb")
    public Integer MemoryMB;

    @JsonProperty("num_cores")
    public Float NumCores;

    @JsonProperty("description")
    public String Description;

    @JsonProperty("instance_type_id")
    public String InstanceTypeId;

    @JsonProperty("is_deprecated")
    public Boolean IsDeprecated;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Id : " + this.Id + ", ");
        stringBuilder.append("MemoryMB : " + this.MemoryMB + '\n');
        stringBuilder.append("NumCores : " + this.NumCores + '\n');
        stringBuilder.append("Description : " + this.Description + '\n');
        stringBuilder.append("InstanceTypeId : " + this.InstanceTypeId + '\n');
        stringBuilder.append("IsDeprecated : " + this.IsDeprecated + '\n');
        return stringBuilder.toString();
    }



}
