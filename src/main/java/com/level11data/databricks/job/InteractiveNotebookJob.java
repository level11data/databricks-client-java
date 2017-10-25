package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.entities.jobs.*;
import com.level11data.databricks.workspace.Notebook;

public class InteractiveNotebookJob extends InteractiveJob {

    public final Notebook Notebook;
    //TODO add BaseParameters

    //TODO add a contsructor that includes notebook parameters
    public InteractiveNotebookJob(JobsClient client,
                                     InteractiveCluster cluster,
                                     long jobId,
                                     JobSettingsDTO jobSettingsDTO,
                                     Notebook notebook) {
        super(client, cluster, jobId, jobSettingsDTO);
        Notebook = notebook;
    }

    //TODO add a contsructor that includes notebook parameters
    public InteractiveNotebookJob(JobsClient client, JobDTO jobDTO)
            throws JobConfigException, ClusterConfigException, HttpException {
        super(client, client.Session.getCluster(jobDTO.Settings.ExistingClusterId), jobDTO.JobId, jobDTO.Settings);

        JobValidation.validateInteractiveNotebookJob(jobDTO);

        Notebook notebook = new Notebook(jobDTO.Settings.NotebookTask.NotebookPath);
        Notebook = notebook;
    }

    public InteractiveNotebookJobRun run() throws HttpException, JobRunException {
        //simple run request with no parameter overrides
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new InteractiveNotebookJobRun(_client, jobRun);
    }
}
