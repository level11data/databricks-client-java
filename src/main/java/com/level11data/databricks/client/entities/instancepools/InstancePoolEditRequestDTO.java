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
public class InstancePoolEditRequestDTO {

    //required, non-editable
    @JsonProperty("instance_pool_id")
    public String InstancePoolId;

    //required, non-editable
    @JsonProperty("node_type_id")
    public String NodeTypeId;

    //required, non-editable
    @JsonProperty("disk_spec")
    public DiskSpecDTO DiskSpec;

    //required, editable
    @JsonProperty("instance_pool_name")
    public String InstancePoolName;

    //optional, editable
    @JsonProperty("min_idle_instances")
    public Integer MinIdleInstances;

    //optional, editable
    @JsonProperty("max_capacity")
    public Integer MaxCapacity;

    //optional, editable
    @JsonProperty("idle_instance_autotermination_minutes")
    public Integer IdleInstanceAutoTerminationMinutes;

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
