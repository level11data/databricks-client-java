package com.level11data.databricks.entities.jobs;

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
public class RunDTO {

    @JsonProperty("job_id")
    public long JobId;

    @JsonProperty("run_id")
    public long RunId;

    @JsonProperty("creator_user_name")
    public String CreatorUserName;

    @JsonProperty("number_in_job")
    public long NumberInJob;

    @JsonProperty("original_attempt_run_id")
    public long OriginalAttemptRunId;

    @JsonProperty("state")
    public RunStateDTO State;

    @JsonProperty("schedule")
    public CronScheduleDTO Schedule;

    @JsonProperty("task")
    public JobTaskDTO Task;

    @JsonProperty("cluster_spec")
    public ClusterSpecDTO ClusterSpec;

    @JsonProperty("cluster_instance")
    public ClusterInstanceDTO ClusterInstance;

    @JsonProperty("overriding_parameters")
    public RunParametersDTO OverridingParameters;

    @JsonProperty("start_time")
    public long StartTime;

    @JsonProperty("setup_duration")
    public Long SetupDuration;

    @JsonProperty("execution_duration")
    public Long ExecutionDuration;

    @JsonProperty("cleanup_duration")
    public Long CleanupDuration;

    @JsonProperty("trigger")
    public String TriggerType;

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
