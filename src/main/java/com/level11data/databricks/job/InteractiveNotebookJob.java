package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.workspace.Notebook;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InteractiveNotebookJob extends InteractiveJob {

    public final Notebook Notebook;
    public final Map<String,String> BaseParameters;

    /**
     * Create a Notebook Job on an Interactive Cluster
     *
     * @param client
     * @param cluster
     * @param jobId
     * @param jobSettingsDTO
     * @param notebook
     */
    public InteractiveNotebookJob(JobsClient client,
                                     InteractiveCluster cluster,
                                     long jobId,
                                     JobSettingsDTO jobSettingsDTO,
                                     Notebook notebook) {
        super(client, cluster, jobId, jobSettingsDTO);
        Notebook = notebook;
        if(jobSettingsDTO.NotebookTask.BaseParameters == null) {
            BaseParameters = Collections.unmodifiableMap(new HashMap<String, String>());
        } else {
            BaseParameters = Collections.unmodifiableMap(jobSettingsDTO.NotebookTask.BaseParameters);
        }
    }

    /**
     * Create a Notebook Job on an Interactive Cluster using a Job DTO object.
     *
     * @param client
     * @param jobDTO
     * @throws JobConfigException
     * @throws ClusterConfigException
     * @throws HttpException
     */
    public InteractiveNotebookJob(JobsClient client, JobDTO jobDTO)
            throws JobConfigException, ClusterConfigException, HttpException {
        super(client, client.Session.getCluster(jobDTO.Settings.ExistingClusterId), jobDTO.JobId, jobDTO.Settings);

        //Validate that the DTO represents an InteractiveNotebookJob
        JobValidation.validateInteractiveNotebookJob(jobDTO);

        Notebook notebook = new Notebook(jobDTO.Settings.NotebookTask.NotebookPath);
        Notebook = notebook;

        if(jobDTO.Settings.NotebookTask.BaseParameters != null) {
            BaseParameters = Collections.unmodifiableMap(jobDTO.Settings.NotebookTask.BaseParameters);
        } else {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        }

    }

    public InteractiveNotebookJobRun run() throws HttpException, JobRunException {
        //simple run request with no parameter overrides
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new InteractiveNotebookJobRun(_client, jobRun);
    }

    public InteractiveNotebookJobRun run(Map<String,String> overrideParameters) throws HttpException, JobRunException {
        //simple run request with no parameter overrides
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;
        runRequestDTO.NotebookParams = overrideParameters;
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new InteractiveNotebookJobRun(_client, jobRun);
    }
}
