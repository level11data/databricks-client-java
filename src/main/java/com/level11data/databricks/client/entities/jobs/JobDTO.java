package com.level11data.databricks.client.entities.jobs;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
public class JobDTO {
    @JsonProperty("job_id")
    public long JobId;

    @JsonProperty("creator_user_name")
    public String CreatorUserName;

    @JsonProperty("settings")
    public JobSettingsDTO Settings;

    @JsonProperty("created_time")
    public long CreatedTime;

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
    public boolean isInteractive() { return Settings.ExistingClusterId != null; }

    @JsonIgnore
    public boolean isAutomated() { return Settings.NewCluster != null; }

    @JsonIgnore
    public boolean isNotebookJob() { return Settings.NotebookTask != null; }

    @JsonIgnore
    public boolean isJarJob() { return Settings.SparkJarTask != null; }

    @JsonIgnore
    public boolean isEggJob() { return Settings.SparkPythonTask != null; }

    @JsonIgnore
    public boolean isSparkSubmitJob() {return Settings.SparkSubmitTask != null; }
}
