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
import java.util.List;

public class AutomatedPythonJob extends AutomatedJob {

    private JobsClient _client;
    public final PythonScript PythonScript;
    public final String[] Parameters;

    public AutomatedPythonJob(JobsClient client,
                              PythonScript pythonScript,
                              JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        this(client, pythonScript, jobSettingsDTO, null);
    }

    public AutomatedPythonJob(JobsClient client,
                              PythonScript pythonScript,
                              JobSettingsDTO jobSettingsDTO,
                              List<Library> libraries) throws JobConfigException {
        super(client, null, jobSettingsDTO, libraries);
        _client = client;

        //Validate DTO for this Job Type
        JobValidation.validateAutomatedPythonJob(jobSettingsDTO);

        PythonScript = pythonScript; //maintain object reference from builder
        Parameters = jobSettingsDTO.SparkPythonTask.Parameters;
    }

    public AutomatedPythonJobRun run() throws JobRunException {
        return run(null);
    }

    public AutomatedPythonJobRun run(List<String> overrideParameters) throws JobRunException {
        try {
            RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
            runRequestDTO.JobId = this.Id;

            if(overrideParameters != null) {
                runRequestDTO.PythonParams = overrideParameters.toArray(new String[overrideParameters.size()]);
            }
            RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
            RunDTO jobRun = _client.getRun(response.RunId);
            return new AutomatedPythonJobRun(_client, PythonScript, jobRun);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }


}
