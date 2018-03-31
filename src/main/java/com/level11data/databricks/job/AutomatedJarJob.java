package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.jobs.RunNowRequestDTO;
import com.level11data.databricks.client.entities.jobs.RunNowResponseDTO;
import com.level11data.databricks.job.run.AutomatedJarJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;

import java.net.URISyntaxException;
import java.util.List;

public class AutomatedJarJob extends AutomatedJob {

    private JobsClient _client;
    public final String MainClassName;
    public final String[] Parameters;

    public AutomatedJarJob(JobsClient client,
                           JobSettingsDTO jobSettingsDTO,
                           List<Library> libraries) throws HttpException, JobConfigException, URISyntaxException, LibraryConfigException  {
        super(client, client.createJob(jobSettingsDTO), jobSettingsDTO, libraries);
        _client = client;

        //Validate that the DTO represents an AutomatedNotebookJob
        JobValidation.validateAutomatedJarJob(jobSettingsDTO);

        MainClassName = jobSettingsDTO.SparkJarTask.MainClassName;
        Parameters = jobSettingsDTO.SparkJarTask.Parameters;
    }

    public AutomatedJarJobRun run() throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        return run(null);
    }

    public AutomatedJarJobRun run(List<String> overrideParameters) throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;

        if(overrideParameters != null) {
            runRequestDTO.JarParams = overrideParameters.toArray(new String[overrideParameters.size()]);
        }
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new AutomatedJarJobRun(_client, jobRun);
    }


}
