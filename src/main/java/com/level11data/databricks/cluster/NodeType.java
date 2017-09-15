package com.level11data.databricks.cluster;

public class NodeType {
    public final String Id;
    public final Integer MemoryMB;
    public final Float NumCores;
    public final String Description;
    public final String InstanceTypeId;
    public final Boolean IsDeprecated;

    public NodeType(com.level11data.databricks.entities.clusters.NodeType nodeTypeInfo) {
        Id = nodeTypeInfo.Id;
        MemoryMB = nodeTypeInfo.MemoryMB;
        NumCores = nodeTypeInfo.NumCores;
        Description = nodeTypeInfo.Description;
        InstanceTypeId = nodeTypeInfo.InstanceTypeId;
        IsDeprecated = nodeTypeInfo.IsDeprecated;
    }
}
