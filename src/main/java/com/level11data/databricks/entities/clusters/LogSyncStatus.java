package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogSyncStatus {
    @JsonProperty("last_attempted")
    public BigInteger LastAttempted;

    @JsonProperty("last_exception")
    public String LastException;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("LastAttempted : " + this.LastAttempted + ", ");
        stringBuilder.append("LastException : " + this.LastException + '\n');
        return stringBuilder.toString();
    }
}
