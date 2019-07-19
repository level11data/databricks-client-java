package com.level11data.databricks.instancepool;

import com.level11data.databricks.client.entities.instancepools.DiskSpecDTO;

public class DiskSpec {

    public final int DiskCount;
    public final int DiskSize;
    public final DiskVolumeType DiskType;

    public DiskSpec(DiskSpecDTO diskSpecDTO) {
        DiskCount = diskSpecDTO.DiskCount;
        DiskSize = diskSpecDTO.DiskSize;
        DiskType = DiskVolumeType.valueOf(diskSpecDTO.DiskType.EbsVolumeType);
    }

}
