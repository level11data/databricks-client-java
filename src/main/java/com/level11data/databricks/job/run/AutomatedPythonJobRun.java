package com.level11data.databricks.job.run;


import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.AutomatedPythonJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.job.PythonScript;
import java.util.List;

public class AutomatedPythonJobRun extends AbstractAutomatedJobRun implements JobRun {
    private JobsClient _client;
    private long _jobId;
    private AutomatedPythonJob _job;

    private final PythonScript _pythonScript;
    private final List<String> _baseParameters;
    private final List<String> _overridingParameters;

    public AutomatedPythonJobRun(JobsClient client, PythonScript pythonScript, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        _jobId = runDTO.JobId;

        _pythonScript = pythonScript;

        if(!runDTO.isPythonJob()) {
            throw new JobRunException("Job Run is not configured as a Python Job");
        }

        //Set Base Parameters of Run
        _baseParameters = this.getBaseParametersAsList();

        //Set Overriding Parameters of Run
        _overridingParameters = this.getOverridingParametersAsList();
    }

    public AutomatedPythonJob getJob() throws JobRunException {
        if(_job == null){
            try{
                return (AutomatedPythonJob)_client.Session.getJob(_jobId);
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

    public PythonScript getPythonScript() {
        return _pythonScript;
    }
}
