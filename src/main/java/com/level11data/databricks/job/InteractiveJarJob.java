package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.jobs.RunNowRequestDTO;
import com.level11data.databricks.client.entities.jobs.RunNowResponseDTO;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.library.LibraryConfigException;
import java.net.URISyntaxException;
import java.util.List;

public class InteractiveJarJob extends InteractiveJob {
    private JobsClient _client;

    public final String MainClassName;
    public final String[] Parameters;

    public InteractiveJarJob(JobsClient client,
                             InteractiveCluster cluster,
                             JobSettingsDTO jobSettingsDTO)
            throws HttpException, JobConfigException, LibraryConfigException, URISyntaxException {
        super(client, cluster, client.createJob(jobSettingsDTO), jobSettingsDTO);
        _client = client;

        //Validate that the DTO represents an InteractiveJarJob
        JobValidation.validateInteractiveJarJob(jobSettingsDTO);

        MainClassName = jobSettingsDTO.SparkJarTask.MainClassName;
        Parameters = jobSettingsDTO.SparkJarTask.Parameters;
    }

    public InteractiveJarJobRun run() throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        return run(null);
    }

    public InteractiveJarJobRun run(List<String> overrideParameters) throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;

        if(overrideParameters != null) {
            runRequestDTO.JarParams = overrideParameters.toArray(new String[overrideParameters.size()]);
        }
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new InteractiveJarJobRun(_client, jobRun);
    }


}
