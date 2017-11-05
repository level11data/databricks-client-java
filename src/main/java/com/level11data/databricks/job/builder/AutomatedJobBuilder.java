package com.level11data.databricks.job.builder;

import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;

public abstract class AutomatedJobBuilder extends JobBuilder {
    private AutomatedClusterBuilder _clusterBuilder ;
    private ClusterSpec _clusterSpec;

    public AutomatedJobBuilder() {
        super();
    }

    public AutomatedJobBuilder withClusterSpec(ClusterSpec clusterSpec) {
        _clusterSpec = clusterSpec;
        return this;
    }

    public AutomatedClusterBuilder withClusterSpec(int numWorkers) {
        if (_clusterBuilder == null) {
            _clusterBuilder = new AutomatedClusterBuilder(this, numWorkers);
        }
        return _clusterBuilder;
    }


}
