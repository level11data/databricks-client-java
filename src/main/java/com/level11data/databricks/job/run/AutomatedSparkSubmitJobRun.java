package com.level11data.databricks.job.run;


import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;

import java.util.List;

public class AutomatedSparkSubmitJobRun extends AbstractAutomatedJobRun implements JobRun {
    private JobsClient _client;

    public final List<String> BaseParameters;
    public final List<String> OverridingParameters;

    public AutomatedSparkSubmitJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;

        if(!runDTO.isSparkSubmitJob()) {
            throw new JobRunException("Job Run is not configured as a Spark Submit Job");
        }

        //Set Base Parameters of Run
        BaseParameters = this.getBaseParametersAsList();

        //Set Overriding Parameters of Run
        OverridingParameters = this.getOverridingParametersAsList();
    }

}
