package com.level11data.databricks.client.entities.jobs;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
public class JobSettingsDTO {
    @JsonProperty("existing_cluster_id")
    public String ExistingClusterId;

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

    @JsonProperty("name")
    public String Name;

    @JsonProperty("libraries")
    public LibraryDTO[] Libraries;

    @JsonProperty("email_notifications")
    public JobEmailNotificationsDTO EmailNotifications;

    @JsonProperty("max_retries")
    public Integer MaxRetries;

    @JsonProperty("min_retry_interval_millis")
    public Integer MinRetryIntervalMillis;

    @JsonProperty("retry_on_timeout")
    public boolean RetryOnTimeout;

    @JsonProperty("schedule")
    public CronScheduleDTO Schedule;

    @JsonProperty("max_concurrent_runs")
    public Integer MaxConcurrentRuns;

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

    @JsonIgnore
    public boolean isInteractive() { return ExistingClusterId != null; }

    @JsonIgnore
    public boolean isAutomated() { return NewCluster != null; }

    @JsonIgnore
    public boolean isNotebookJob() { return NotebookTask != null; }

    @JsonIgnore
    public boolean isJarJob() { return SparkJarTask != null; }

    @JsonIgnore
    public boolean isEggJob() { return SparkPythonTask != null; }

    @JsonIgnore
    public boolean isSparkSubmitJob() {return SparkSubmitTask != null; }
}
