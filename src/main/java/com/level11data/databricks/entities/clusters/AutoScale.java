package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AutoScale {
    @JsonProperty("min_workers")
    public Integer MinWorkers;

    @JsonProperty("max_workers")
    public Integer MaxWorkers;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("MinWorkers : " + this.MinWorkers + ", ");
        stringBuilder.append("MaxWorkers : " + this.MaxWorkers + '\n');
        return stringBuilder.toString();
    }

}
