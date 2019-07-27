package com.level11data.databricks.client.entities.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RunsSubmitRequestDTO {

    @JsonProperty("existing_cluster_id")
    public String ExistingClusterId;

    @JsonProperty("instance_pool_id")
    public String InstancePoolId;

    @JsonProperty("new_cluster")
    public ClusterInfoDTO NewCluster;

    @JsonProperty("notebook_task")
    public NotebookTaskDTO NotebookTask;

    @JsonProperty("spark_jar_task")
    public SparkJarTaskDTO SparkJarTask;

    @JsonProperty("spark_python_task")
    public PythonTaskDTO SparkPythonTask;

    @JsonProperty("spark_submit_task")
    public SparkSubmitTaskDTO SparkSubmitTask;

    @JsonProperty("run_name")
    public String RunName;

    @JsonProperty("libraries")
    public LibraryDTO[] Libraries;

    @JsonProperty("timeout_seconds")
    public Integer TimeoutSeconds;

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
