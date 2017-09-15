package com.level11data.databricks.cluster;

public class SparkNodeAwsAttributes {
    public final Boolean IsSpot;

    public SparkNodeAwsAttributes(com.level11data.databricks.entities.clusters.SparkNodeAwsAttributes awsAttribInfo) {
        IsSpot = awsAttribInfo.IsSpot;
    }
}
