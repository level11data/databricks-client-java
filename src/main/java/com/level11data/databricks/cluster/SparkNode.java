package com.level11data.databricks.cluster;

import com.level11data.databricks.entities.clusters.SparkNodeDTO;

import java.math.BigInteger;

public class SparkNode {
    public final NodeType NodeType;
    public final String PrivateIP;
    public final String PublicDNS;
    public final String NodeId;
    public final String InstanceId;
    public final BigInteger StartTimestamp;
    public final SparkNodeAwsAttributes NodeAwsAttributes;
    public final String HostPrivateIP;

    public SparkNode(SparkNodeDTO sparkNodeDTOInfo, NodeType nodeType) {
        PrivateIP = sparkNodeDTOInfo.PrivateIP;
        PublicDNS = sparkNodeDTOInfo.PublicDNS;
        NodeId = sparkNodeDTOInfo.NodeId;
        InstanceId = sparkNodeDTOInfo.InstanceId;
        StartTimestamp = sparkNodeDTOInfo.StartTimestamp;
        NodeAwsAttributes = new SparkNodeAwsAttributes(sparkNodeDTOInfo.NodeAwsAttributes);
        HostPrivateIP = sparkNodeDTOInfo.HostPrivateIP;
        NodeType = nodeType;
    }
}
