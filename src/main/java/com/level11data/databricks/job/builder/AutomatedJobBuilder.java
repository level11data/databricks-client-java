package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;

public abstract class AutomatedJobBuilder extends JobBuilder {
    private ClusterInfoDTO _clusterInfo;
    private AutomatedClusterBuilder _clusterBuilder ;

    public AutomatedJobBuilder() {
        super();
    }

    public AutomatedJobBuilder withClusterInfo(ClusterInfoDTO clusterInfoDTO) {
        _clusterInfo = clusterInfoDTO;
        return this;
    }

    @Override
    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
        jobSettingsDTO = super.applySettings(jobSettingsDTO);
        jobSettingsDTO.NewCluster = _clusterInfo;

        //TODO parse cron schedule expression
        //https://stackoverflow.com/questions/3641575/how-to-get-cron-expression-given-job-name-and-group-name
        //jobSettingsDTO.Schedule = ;

        //TODO add libraries to DTO

        return jobSettingsDTO;
    }

    public AutomatedClusterBuilder withClusterSpec(int numWorkers) {
        if (_clusterBuilder == null) {
            _clusterBuilder = new AutomatedClusterBuilder(this, numWorkers);
        }
        return _clusterBuilder;
    }
}
