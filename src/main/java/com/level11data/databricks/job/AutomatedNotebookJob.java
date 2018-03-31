package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.job.run.AutomatedNotebookJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.workspace.Notebook;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AutomatedNotebookJob extends AutomatedJob {

    private JobsClient _client;
    public final com.level11data.databricks.workspace.Notebook Notebook;
    public final Map<String,String> BaseParameters;

    /**
     * Create a Notebook Job on an Automated Cluster
     *
     * @param client
     * @param jobSettingsDTO
     * @param notebook
     */
    public AutomatedNotebookJob(JobsClient client,
                                JobSettingsDTO jobSettingsDTO,
                                Notebook notebook) throws HttpException, JobConfigException, URISyntaxException, LibraryConfigException {
        super(client, client.createJob(jobSettingsDTO), jobSettingsDTO);
        _client = client;

        //Validate that the DTO represents an AutomatedNotebookJob
        JobValidation.validateAutomatedNotebookJob(jobSettingsDTO);

        Notebook = notebook;

        if(jobSettingsDTO.NotebookTask.BaseParameters == null) {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        } else {
            BaseParameters = Collections.unmodifiableMap(jobSettingsDTO.NotebookTask.BaseParameters);
        }
    }

    /**
     * Create a Notebook Job on an Interactive Cluster using a Job DTO object
     *
     * @param client
     * @param jobDTO
     * @throws JobConfigException
     * @throws ClusterConfigException
     * @throws HttpException
     */
    public AutomatedNotebookJob(JobsClient client, JobDTO jobDTO)
            throws JobConfigException, ClusterConfigException, HttpException, URISyntaxException, LibraryConfigException {
        super(client, jobDTO.JobId, jobDTO.Settings);

        _client = client;

        //Validate that the DTO represents an AutomatedNotebookJob
        JobValidation.validateAutomatedNotebookJob(jobDTO);

        Notebook = new Notebook(jobDTO.Settings.NotebookTask.NotebookPath);

        if(jobDTO.Settings.NotebookTask.BaseParameters != null) {
            BaseParameters = Collections.unmodifiableMap(jobDTO.Settings.NotebookTask.BaseParameters);
        } else {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        }
    }

    public AutomatedNotebookJobRun run() throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        //simple run request with no parameter overrides
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new AutomatedNotebookJobRun(_client, jobRun);
    }

    public AutomatedNotebookJobRun run(Map<String,String> overrideParameters) throws HttpException, JobRunException, LibraryConfigException, URISyntaxException {
        //simple run request with no parameter overrides
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;
        runRequestDTO.NotebookParams = overrideParameters;

        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new AutomatedNotebookJobRun(_client, jobRun);
    }

}
