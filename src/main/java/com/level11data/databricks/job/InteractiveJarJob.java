package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.job.run.InteractiveJarJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.Library;

import java.util.List;

public class InteractiveJarJob extends AbstractInteractiveJob implements StandardJob {
    private JobsClient _client;

    public final String MainClassName;
    public final String[] Parameters;

    public InteractiveJarJob(JobsClient client,
                             InteractiveCluster cluster,
                             JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        this(client, cluster, jobSettingsDTO, null);
    }

    public InteractiveJarJob(JobsClient client,
                    InteractiveCluster cluster,
                    JobSettingsDTO jobSettingsDTO,
                    List <Library> libraries) throws JobConfigException {
            super(client, cluster, null, jobSettingsDTO, libraries);
            _client = client;

            //Validate DTO for this Job Type
            JobValidation.validateInteractiveJarJob(jobSettingsDTO);

            MainClassName = jobSettingsDTO.SparkJarTask.MainClassName;
            Parameters = jobSettingsDTO.SparkJarTask.Parameters;
    }

    public InteractiveJarJob(JobsClient client,
                             InteractiveCluster cluster,
                             JobDTO jobDTO) throws JobConfigException {
        this(client, cluster, jobDTO, null);
    }

    public InteractiveJarJob(JobsClient client,
                             InteractiveCluster cluster,
                             JobDTO jobDTO,
                             List <Library> libraries) throws JobConfigException {
        super(client, cluster, jobDTO.JobId, jobDTO.Settings, libraries);
        _client = client;

        //Validate DTO for this Job Type
        JobValidation.validateInteractiveJarJob(jobDTO);

        MainClassName = jobDTO.Settings.SparkJarTask.MainClassName;
        Parameters = jobDTO.Settings.SparkJarTask.Parameters;
    }

    public InteractiveJarJobRun run() throws JobRunException {
        return run(null);
    }

    public InteractiveJarJobRun run(List<String> overrideParameters) throws JobRunException {
        try {
            RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
            runRequestDTO.JobId = this.Id;

            if(overrideParameters != null) {
                runRequestDTO.JarParams = overrideParameters.toArray(new String[overrideParameters.size()]);
            }
            RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
            RunDTO jobRun = _client.getRun(response.RunId);
            return new InteractiveJarJobRun(_client, jobRun);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }

    private long createJob(JobsClient client, JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        try {
            return client.createJob(jobSettingsDTO);
        } catch(HttpException e) {
            throw new JobConfigException(e);
        }
    }
}
