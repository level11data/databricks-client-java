package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.job.JobConfigException;

public abstract class AbstractAutomatedJobBuilder extends AbstractJobBuilder {
    private AutomatedClusterBuilder _clusterBuilder ;
    private ClusterSpec _clusterSpec;

    public AbstractAutomatedJobBuilder() {
        super();
    }

    public AbstractAutomatedJobBuilder withClusterSpec(ClusterSpec clusterSpec) {
        _clusterSpec = clusterSpec;
        return this;
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        if(_clusterSpec == null) {
            throw new JobConfigException("No ClusterSpec was supplied for Automated AbstractJob");  //TODO ClusterSpec should be part of the signature
        } else {
            jobSettingsDTO.NewCluster = _clusterSpec.getClusterInfo();
        }

        return jobSettingsDTO;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //no op
    }
}
