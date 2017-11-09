package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.AutoScaleDTO;

public class AutoScale {
    public final Integer MinWorkers;
    public final Integer MaxWorkers;

    public AutoScale(Integer minWorkers, Integer maxWorkers) {
        MinWorkers = minWorkers;
        MaxWorkers = maxWorkers;
    }

    public AutoScale(AutoScaleDTO autoScaleDTOInfo) {
        MinWorkers = autoScaleDTOInfo.MinWorkers;
        MaxWorkers = autoScaleDTOInfo.MaxWorkers;
    }
}
