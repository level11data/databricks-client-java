package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterLogConf {
    @JsonProperty("dbfs")
    public DbfsStorageInfo DBFS;

    @JsonProperty("s3")
    public S3StorageInfo S3;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DBFS : " + this.DBFS.toString() + ", ");
        return stringBuilder.toString();
    }
}
