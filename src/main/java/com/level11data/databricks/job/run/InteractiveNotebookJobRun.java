package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.job.util.JobRunHelper;
import com.level11data.databricks.workspace.Notebook;
import com.level11data.databricks.workspace.WorkspaceConfigException;

import java.util.Map;

public class InteractiveNotebookJobRun extends AbstractInteractiveJobRun implements NotebookJobRun {
    private JobsClient _client;
    private long _jobId;
    private InteractiveNotebookJob _job;

    private String _jobRunOutputResult;

    private final Notebook _notebook;
    private final Map<String,String> _baseParameters;
    private final Map<String,String> _overridingParameters;

    public InteractiveNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        _jobId = runDTO.JobId;

        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a AbstractNotebook Job");
        }

        try {
            _notebook = _client.Session.getNotebook(runDTO.Task.NotebookTask.NotebookPath);
        } catch(WorkspaceConfigException e) {
            throw new JobRunException(e);
        }

        //Set Base Parameters of Run
        _baseParameters = this.getBaseParametersAsMap();

        //Set Overriding Parameters of Run
        _overridingParameters = this.getOverridingParametersAsMap();
    }

    public String getOutput() throws JobRunException {
        if(_jobRunOutputResult != null) {
            return _jobRunOutputResult;
        }

        try {
            return JobRunHelper.getJobRunOutput(_client, getRunId());
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }

    public InteractiveNotebookJob getJob() throws JobRunException {
        if(_job == null) {
            try{
                return (InteractiveNotebookJob)_client.Session.getJob(_jobId);
            }catch(JobConfigException e){
                throw new JobRunException(e);
            }
        }
        return _job;
    }

    public Notebook getNotebook() {
        return _notebook;
    }

    public Map<String,String> getBaseParameters() {
        return _baseParameters;
    }

    public Map<String,String> getOverridingParameters() {
        return _overridingParameters;
    }
}
