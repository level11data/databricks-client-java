package com.level11data.databricks.job.run;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.JobConfigException;

import java.util.List;

public class AutomatedJarJobRun extends AbstractAutomatedJobRun implements JobRun {
    private JobsClient _client;
    private long _jobId;
    private AutomatedJarJob _job;
    private final String _mainClassName;
    private final List<String> _baseParameters;
    private final List<String> _overridingParameters;

    public AutomatedJarJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        _jobId = runDTO.JobId;

        if(!runDTO.isJarJob()) {
            throw new JobRunException("Job Run is not configured as a JAR Job");
        }

        _mainClassName = runDTO.Task.SparkJarTask.MainClassName;

        //Set Base Parameters of Run
        _baseParameters = this.getBaseParametersAsList();

        //Set Overriding Parameters of Run
        _overridingParameters = this.getOverridingParametersAsList();
    }

    public AutomatedJarJob getJob() throws JobRunException {
        if(_job == null) {
            try{
                return (AutomatedJarJob)_client.Session.getJob(_jobId);
            }catch(JobConfigException e){
                throw new JobRunException(e);
            }
        }
        return _job;
    }

    public String getMainClassName() {
        return _mainClassName;
    }

    public List<String> getBaseParameters() {
        return _baseParameters;
    }

    public List<String> getOverridingParameters() {
        return _overridingParameters;
    }
}
