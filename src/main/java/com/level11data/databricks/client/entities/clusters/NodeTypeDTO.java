package com.level11data.databricks.client.entities.clusters;

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
public class NodeTypeDTO {
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
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            return ow.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Could Not Marshal Object to JSON";
        }
    }
}
