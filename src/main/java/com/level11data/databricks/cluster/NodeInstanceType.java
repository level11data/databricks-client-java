package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.NodeInstanceTypeDTO;

public class NodeInstanceType {

    public final String InstanceTypeId;
    public final int LocalDisks;
    public final float LocalDiskSizeGb;

    NodeInstanceType(NodeInstanceTypeDTO nodeInstanceTypeDTO) {
        InstanceTypeId = nodeInstanceTypeDTO.InstanceTypeId;
        LocalDisks = nodeInstanceTypeDTO.LocalDisks;
        LocalDiskSizeGb = nodeInstanceTypeDTO.LocalDiskSizeGb;
    }

}
