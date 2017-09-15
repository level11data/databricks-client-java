package com.level11data.databricks.entities.libraries;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Library {

    @JsonProperty("jar")
    public String Jar;

    @JsonProperty("egg")
    public String Egg;

    @JsonProperty("pypi")
    public PythonPyPiLibrary PyPi;

    @JsonProperty("maven")
    public MavenLibrary Maven;

}
