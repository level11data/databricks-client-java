package com.level11data.databricks.instancepool;

import com.level11data.databricks.client.entities.instancepools.InstancePoolStatsDTO;

public class InstancePoolStats {
    public final int UsedCount;
    public final int IdleCount;
    public final int PendingUsedCount;
    public final int PendingIdleCount;

    public InstancePoolStats(InstancePoolStatsDTO instancePoolStatsDTO) {
        UsedCount = instancePoolStatsDTO.UsedCount;
        IdleCount = instancePoolStatsDTO.IdleCount;
        PendingUsedCount = instancePoolStatsDTO.PendingUsedCount;
        PendingIdleCount = instancePoolStatsDTO.PendingIdleCount;
    }
}
