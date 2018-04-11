package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;
import com.level11data.databricks.job.JobConfigException;

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

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);
        jobSettingsDTO.NewCluster = _clusterSpec.getClusterInfo();

        return jobSettingsDTO;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        if(jobSettingsDTO.NewCluster == null) {
            throw new JobConfigException("No ClusterSpec was supplied for Automated Job");
        }
    }
}
