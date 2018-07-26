package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.job.run.AutomatedSparkSubmitJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.Library;

import java.util.List;

public class AutomatedSparkSubmitJob extends AbstractAutomatedJob implements StandardJob {

    private JobsClient _client;
    public final String[] Parameters;

    public AutomatedSparkSubmitJob(JobsClient client,
                                   JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super(client, null, jobSettingsDTO, null);
        _client = client;

        //Validate the DTO for this Job Type
        JobValidation.validateAutomatedSparkSubmitJob(jobSettingsDTO);

        Parameters = jobSettingsDTO.SparkSubmitTask.Parameters;
    }

    public AutomatedSparkSubmitJob(JobsClient client,
                                   JobDTO jobDTO) throws JobConfigException {
        super(client, jobDTO.JobId, jobDTO.Settings, null);
        _client = client;

        //Validate the DTO for this Job Type
        JobValidation.validateAutomatedSparkSubmitJob(jobDTO);

        Parameters = jobDTO.Settings.SparkSubmitTask.Parameters;
    }


    public AutomatedSparkSubmitJobRun run() throws JobRunException {
        return run(null);
    }

    public AutomatedSparkSubmitJobRun run(List<String> overrideParameters) throws JobRunException {
        try {
            RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
            runRequestDTO.JobId = this.Id;

            if(overrideParameters != null) {
                runRequestDTO.JarParams = overrideParameters.toArray(new String[overrideParameters.size()]);
            }
            RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
            RunDTO jobRun = _client.getRun(response.RunId);
            return new AutomatedSparkSubmitJobRun(_client, jobRun);
        } catch (HttpException e) {
            throw new JobRunException(e);
        }
    }


}
