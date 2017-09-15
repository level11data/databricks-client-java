package com.level11data.databricks.entities.libraries;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterLibraryRequest {

    @JsonProperty("cluster_id")
    public String ClusterId;

    @JsonProperty("libraries")
    public Library[] Libraries;

}
