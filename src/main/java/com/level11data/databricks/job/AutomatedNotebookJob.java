package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.job.run.AutomatedNotebookJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.workspace.Notebook;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AutomatedNotebookJob extends AbstractAutomatedJob implements NotebookJob {

    private JobsClient _client;
    public final com.level11data.databricks.workspace.Notebook Notebook;
    public final Map<String,String> BaseParameters;

    /**
     * Create a Notebook AbstractJob on an Automated AbstractCluster
     *
     * @param client
     * @param jobSettingsDTO
     * @param notebook
     */
    public AutomatedNotebookJob(JobsClient client,
                                JobSettingsDTO jobSettingsDTO,
                                Notebook notebook) throws JobConfigException {
        super(client, null, jobSettingsDTO);
        _client = client;

        //Validate DTO for this AbstractJob Type
        JobValidation.validateAutomatedNotebookJob(jobSettingsDTO);

        Notebook = notebook;

        if(jobSettingsDTO.NotebookTask.BaseParameters == null) {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        } else {
            BaseParameters = Collections.unmodifiableMap(jobSettingsDTO.NotebookTask.BaseParameters);
        }
    }

    /**
     * Create a Notebook AbstractJob on an Automated AbstractCluster using a AbstractJob DTO object
     *
     * @param client
     * @param jobDTO
     * @throws JobConfigException
     */
    public AutomatedNotebookJob(JobsClient client, JobDTO jobDTO)
            throws JobConfigException {
        super(client, jobDTO.JobId, jobDTO.Settings);

        _client = client;

        //Validate DTO for this Job Type
        JobValidation.validateAutomatedNotebookJob(jobDTO);

        Notebook = new Notebook(jobDTO.Settings.NotebookTask.NotebookPath);

        if(jobDTO.Settings.NotebookTask.BaseParameters != null) {
            BaseParameters = Collections.unmodifiableMap(jobDTO.Settings.NotebookTask.BaseParameters);
        } else {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        }
    }

    public AutomatedNotebookJobRun run() throws JobRunException {
        return run(null);
    }

    public AutomatedNotebookJobRun run(Map<String,String> overrideParameters) throws JobRunException {
        try {
            RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
            runRequestDTO.JobId = this.Id;
            runRequestDTO.NotebookParams = overrideParameters;

            RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
            RunDTO jobRun = _client.getRun(response.RunId);
            return new AutomatedNotebookJobRun(_client, jobRun);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }

}
