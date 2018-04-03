package com.level11data.databricks.job.run;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.PythonScript;
import com.level11data.databricks.library.LibraryConfigException;
import java.net.URISyntaxException;
import java.util.List;

public class InteractivePythonJobRun extends InteractiveJobRun {
    public final PythonScript PythonScript;
    public final List<String> BaseParameters;
    public final List<String> OverridingParameters;

    public InteractivePythonJobRun(JobsClient client, PythonScript pythonScript, RunDTO runDTO) throws JobRunException, LibraryConfigException, URISyntaxException {
        super(client, runDTO);
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
