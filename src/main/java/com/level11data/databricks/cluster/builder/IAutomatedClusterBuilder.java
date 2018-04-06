package com.level11data.databricks.cluster.builder;

import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.job.builder.IJobBuilder;

//TODO is this interface necessary since there is only 1 implementation?
public interface IAutomatedClusterBuilder extends IClusterBuilder {

    IJobBuilder addToJob() throws ClusterConfigException;

}
