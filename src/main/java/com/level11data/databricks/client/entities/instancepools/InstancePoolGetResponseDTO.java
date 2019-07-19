package com.level11data.databricks.client.entities.instancepools;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InstancePoolGetResponseDTO {

    @JsonProperty("instance_pool_id")
    public String InstancePoolId;

    @JsonProperty("instance_pool_name")
    public String InstancePoolName;

    @JsonProperty("node_type_id")
    public String NodeTypeId;

    @JsonProperty("min_idle_instances")
    public Integer MinIdleInstances;

    @JsonProperty("max_capacity")
    public Integer MaxCapacity;

    @JsonProperty("idle_instance_autotermination_minutes")
    public Integer IdleInstanceAutoTerminationMinutes;

    @JsonProperty("custom_tags")
    public Map<String, String> CustomTags;

    @JsonProperty("preloaded_spark_versions")
    public String[] PreloadedSparkVersions;

    @JsonProperty("preloaded_docker_images")
    public String[] PreloadedDockerImages;

    @JsonProperty("aws_attributes")
    public AwsAttributesDTO AwsAttributes;

    @JsonProperty("disk_spec")
    public DiskSpecDTO DiskSpec;

    @JsonProperty("enable_elastic_disk")
    public Boolean EnableElasticDisk;

    @JsonProperty("stats")
    public InstancePoolStatsDTO Stats;


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
