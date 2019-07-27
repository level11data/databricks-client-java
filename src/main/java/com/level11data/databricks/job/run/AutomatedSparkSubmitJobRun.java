package com.level11data.databricks.job.run;


import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.AutomatedSparkSubmitJob;
import com.level11data.databricks.job.JobConfigException;

import java.util.List;

public class AutomatedSparkSubmitJobRun extends AbstractAutomatedJobRun implements JobRun {
    private JobsClient _client;
    private long _jobId;
    private AutomatedSparkSubmitJob _job;

    private final List<String> _baseParameters;
    private final List<String> _overridingParameters;

    public AutomatedSparkSubmitJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        _jobId = runDTO.JobId;

        if(!runDTO.isSparkSubmitJob()) {
            throw new JobRunException("Job Run is not configured as a Spark Submit Job");
        }

        //Set Base Parameters of Run
        _baseParameters = this.getBaseParametersAsList();

        //Set Overriding Parameters of Run
        _overridingParameters = this.getOverridingParametersAsList();
    }

    public AutomatedSparkSubmitJob getJob() throws JobRunException {
        if(_job == null) {
            try{
                return (AutomatedSparkSubmitJob)_client.Session.getJob(_jobId);
            }catch(JobConfigException e){
                throw new JobRunException(e);
            }
        }
        return _job;
    }

    public List<String> getBaseParameters() {
        return _baseParameters;
    }

    public List<String> getOverridingParameters() {
        return _overridingParameters;
    }
}
