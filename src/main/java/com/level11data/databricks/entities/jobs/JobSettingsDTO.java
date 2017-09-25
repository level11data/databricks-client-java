package com.level11data.databricks.entities.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.entities.libraries.LibraryDTO;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
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
    public int MaxRetries;

    @JsonProperty("min_retry_interval_millis")
    public int MinRetryIntervalMillis;

    @JsonProperty("retry_on_timeout")
    public boolean RetryOnTimeout;

    @JsonProperty("schedule")
    public CronScheduleDTO Schedule;

    @JsonProperty("max_concurrent_runs")
    public int MaxConcurrentRuns;

    @JsonProperty("timeout_seconds")
    public int TimeoutSeconds;

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
