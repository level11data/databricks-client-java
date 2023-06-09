package com.level11data.databricks.client.entities.jobs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;

public class JobTaskSettings2Dot1DTO {

  @JsonProperty("task_key")
  public String TaskKey;

  @JsonProperty("description")
  public String Description;

  @JsonProperty("existing_cluster_id")
  public String ExistingClusterId;

  @JsonProperty("new_cluster")
  public ClusterInfoDTO NewCluster;

  @JsonProperty("spark_jar_task")
  public SparkJarTaskDTO SparkJarTask;


  @JsonProperty("libraries")
  public LibraryDTO[] Libraries;

  @JsonProperty("timeout_seconds")
  public Integer TimeoutSeconds;

  @JsonProperty("max_retries")
  public Integer MaxRetries;

  @JsonProperty("min_retry_interval_millis")
  public Integer MinRetryIntervalMillis;

  @JsonProperty("retry_on_timeout")
  public boolean RetryOnTimeout;

  @JsonProperty("notebook_task")
  public NotebookTaskDTO NotebookTask;

  @JsonProperty("spark_python_task")
  public PythonTaskDTO SparkPythonTask;

  @JsonProperty("spark_submit_task")
  public SparkSubmitTaskDTO SparkSubmitTask;

  @Override
  public String toString() {
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      return ow.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "Could Not Marshal Object to JSON";
    }
  }

  @JsonIgnore
  public boolean isInteractive() { return ExistingClusterId != null; }

  @JsonIgnore
  public boolean isAutomated() { return NewCluster != null; }

  @JsonIgnore
  public boolean isNotebookJob() { return NotebookTask != null; }

  @JsonIgnore
  public boolean isJarJob() { return SparkJarTask != null; }

  @JsonIgnore
  public boolean isPythonJob() { return SparkPythonTask != null; }

  @JsonIgnore
  public boolean isSparkSubmitJob() {return SparkSubmitTask != null; }
}
