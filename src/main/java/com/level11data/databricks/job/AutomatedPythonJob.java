package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.jobs.RunNowRequestDTO;
import com.level11data.databricks.client.entities.jobs.RunNowResponseDTO;
import com.level11data.databricks.job.run.AutomatedPythonJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import java.net.URISyntaxException;
import java.util.List;

public class AutomatedPythonJob extends AutomatedJob {

    private JobsClient _client;
    public final PythonScript PythonScript;
    public final String[] Parameters;

    public AutomatedPythonJob(JobsClient client,
                              PythonScript pythonScript,
                              JobSettingsDTO jobSettingsDTO,
                              List<Library> libraries) throws HttpException, JobConfigException, URISyntaxException, LibraryConfigException  {
        super(client, client.createJob(jobSettingsDTO), jobSettingsDTO, libraries);
        _client = client;

        //Validate DTO for this Job Type
        JobValidation.validateAutomatedPythonJob(jobSettingsDTO);

        PythonScript = pythonScript; //maintain object reference from builder
        Parameters = jobSettingsDTO.SparkPythonTask.Parameters;
    }

    public AutomatedPythonJobRun run() throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        return run(null);
    }

    public AutomatedPythonJobRun run(List<String> overrideParameters) throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;

        if(overrideParameters != null) {
            runRequestDTO.PythonParams = overrideParameters.toArray(new String[overrideParameters.size()]);
        }
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new AutomatedPythonJobRun(_client, PythonScript, jobRun);
    }


}
