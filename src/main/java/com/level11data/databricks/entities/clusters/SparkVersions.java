package com.level11data.databricks.entities.clusters;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkVersions {

    @JsonProperty("versions")
    public List<SparkVersion> Versions;

    @JsonProperty("default_version_key")
    public String DefaultVersionKey;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Versions : " + this.Versions + '\n');
        stringBuilder.append("DefaultVersionKey : " + this.DefaultVersionKey + '\n');
        return stringBuilder.toString();
    }
}
