package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.NodeTypeDTO;

public class NodeType {
    public final String Id;
    public final int MemoryMB;
    public final float NumCores;
    public final String Description;
    public final String InstanceTypeId;
    public final boolean IsDeprecated;
    public final String Category;
    public final int DisplayOrder;
    public final boolean IsHidden;
    public final boolean IsIoCacheEnabled;
    public final float NumGpus;
    public final boolean SupportClusterTags;
    public final boolean SupportEbsVolumes;
    public final boolean SupportPortForwarding;
    public final NodeInstanceType NodeInstanceType;

    public NodeType(NodeTypeDTO nodeTypeDTOInfo) {
        Id = nodeTypeDTOInfo.Id;
        MemoryMB = nodeTypeDTOInfo.MemoryMB;
        NumCores = nodeTypeDTOInfo.NumCores;
        Description = nodeTypeDTOInfo.Description;
        InstanceTypeId = nodeTypeDTOInfo.InstanceTypeId;
        IsDeprecated = nodeTypeDTOInfo.IsDeprecated;

        Category = nodeTypeDTOInfo.Category;
        DisplayOrder = nodeTypeDTOInfo.DisplayOrder;
        IsHidden = nodeTypeDTOInfo.IsHidden;
        IsIoCacheEnabled = nodeTypeDTOInfo.IsIoCacheEnabled;
        NodeInstanceType = new NodeInstanceType(nodeTypeDTOInfo.NodeInstanceType);
        NumGpus = nodeTypeDTOInfo.NumGpus;
        SupportClusterTags = nodeTypeDTOInfo.SupportClusterTags;
        SupportEbsVolumes = nodeTypeDTOInfo.SupportEbsVolumes;
        SupportPortForwarding = nodeTypeDTOInfo.SupportPortForwarding;
    }
}
