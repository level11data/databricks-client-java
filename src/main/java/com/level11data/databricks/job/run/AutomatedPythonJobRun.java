package com.level11data.databricks.job.run;


import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.PythonScript;
import java.util.List;

public class AutomatedPythonJobRun extends AutomatedJobRun implements IJobRun {
    private JobsClient _client;

    public final PythonScript PythonScript;
    public final List<String> BaseParameters;
    public final List<String> OverridingParameters;

    public AutomatedPythonJobRun(JobsClient client, PythonScript pythonScript, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        PythonScript = pythonScript;

        if(!runDTO.isPythonJob()) {
            throw new JobRunException("Job Run is not configured as a Python Job");
        }

        //Set Base Parameters of Run
        BaseParameters = this.getBaseParametersAsList();

        //Set Overriding Parameters of Run
        OverridingParameters = this.getOverridingParametersAsList();
    }

}
