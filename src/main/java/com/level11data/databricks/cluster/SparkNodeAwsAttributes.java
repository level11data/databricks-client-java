package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.SparkNodeAwsAttributesDTO;

public class SparkNodeAwsAttributes {
    public final Boolean IsSpot;

    public SparkNodeAwsAttributes(SparkNodeAwsAttributesDTO awsAttribInfo) {
        IsSpot = awsAttribInfo.IsSpot;
    }
}
