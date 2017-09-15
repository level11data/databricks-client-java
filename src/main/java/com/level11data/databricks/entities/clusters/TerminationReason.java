package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TerminationReason {
    public enum TerminationCode {
        USER_REQUEST, JOB_FINISHED, INACTIVITY, CLOUD_PROVIDER_SHUTDOWN, COMMUNICATION_LOST,
        CLOUD_PROVIDER_LAUNCH_FAILURE, SPARK_STARTUP_FAILURE, UNEXPECTED_LAUNCH_FAILURE,
        INTERNAL_ERROR
    }

    @JsonProperty("code")
    public TerminationCode TerminationCode;

    @JsonProperty("parameters")
    public Map<String, String> Parameters;

    @Override
    public String toString() {
        return this.toString();
    }
}
