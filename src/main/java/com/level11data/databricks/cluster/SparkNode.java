package com.level11data.databricks.cluster;

import java.math.BigInteger;

public class SparkNode {
    public final String PrivateIP;
    public final String PublicDNS;
    public final String NodeId;
    public final String InstanceId;
    public final BigInteger StartTimestamp;
    public final SparkNodeAwsAttributes NodeAwsAttributes;
    public final String HostPrivateIP;

    public SparkNode(com.level11data.databricks.entities.clusters.SparkNode sparkNodeInfo) {
        PrivateIP = sparkNodeInfo.PrivateIP;
        PublicDNS = sparkNodeInfo.PublicDNS;
        NodeId = sparkNodeInfo.NodeId;
        InstanceId = sparkNodeInfo.InstanceId;
        StartTimestamp = sparkNodeInfo.StartTimestamp;
        NodeAwsAttributes = new SparkNodeAwsAttributes(sparkNodeInfo.NodeAwsAttributes);
        HostPrivateIP = sparkNodeInfo.HostPrivateIP;
    }
}
