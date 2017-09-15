package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class S3StorageInfo {
    @JsonProperty("destination")
    public String Destination;

    @JsonProperty("region")
    public String Region;

    @JsonProperty("endpoint")
    public String Endpoint;

    @JsonProperty("enable_encryption")
    public Boolean EnableEncryption;

    @JsonProperty("encryption_type")
    public String EncryptionType;

    @JsonProperty("kms_key")
    public String KmsKey;

    @JsonProperty("canned_acl")
    public String CannedAcl;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Destination : " + this.Destination + ", ");
        stringBuilder.append("Region : " + this.Region + ", ");
        stringBuilder.append("Endpoint : " + this.Endpoint + ", ");
        stringBuilder.append("EnableEncryptiong : " + this.EnableEncryption + ", ");
        stringBuilder.append("EncryptionType : " + this.EncryptionType + ", ");
        stringBuilder.append("KmsKey : " + this.KmsKey + ", ");
        stringBuilder.append("CannedAcl : " + this.CannedAcl + ", ");
        return stringBuilder.toString();
    }


}
