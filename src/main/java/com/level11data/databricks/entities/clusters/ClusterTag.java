package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterTag {
    @JsonProperty("key")
    public String Key;

    @JsonProperty("value")
    public String Value;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Key : " + this.Key + ", ");
        stringBuilder.append("Value : " + this.Value + '\n');
        return stringBuilder.toString();
    }
}
