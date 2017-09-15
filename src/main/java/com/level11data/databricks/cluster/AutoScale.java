package com.level11data.databricks.cluster;

public class AutoScale {
    public final Integer MinWorkers;
    public final Integer MaxWorkers;

    public AutoScale(Integer minWorkers, Integer maxWorkers) {
        MinWorkers = minWorkers;
        MaxWorkers = maxWorkers;
    }

    public AutoScale(com.level11data.databricks.entities.clusters.AutoScale autoScaleInfo) {
        MinWorkers = autoScaleInfo.MinWorkers;
        MaxWorkers = autoScaleInfo.MaxWorkers;
    }
}
