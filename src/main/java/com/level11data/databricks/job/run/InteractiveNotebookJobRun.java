package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.util.JobRunHelper;
import com.level11data.databricks.workspace.Notebook;
import com.level11data.databricks.workspace.WorkspaceConfigException;

import java.util.Map;

public class InteractiveNotebookJobRun extends AbstractInteractiveJobRun implements NotebookJobRun {
    private JobsClient _client;
    private String _jobRunOutputResult;

    public final Notebook Notebook;
    public final Map<String,String> BaseParameters;
    public final Map<String,String> OverridingParameters;

    public InteractiveNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a AbstractNotebook Job");
        }

        try {
            Notebook = _client.Session.getNotebook(runDTO.Task.NotebookTask.NotebookPath);
        } catch(WorkspaceConfigException e) {
            throw new JobRunException(e);
        }

        //Set Base Parameters of Run
        BaseParameters = this.getBaseParametersAsMap();

        //Set Overriding Parameters of Run
        OverridingParameters = this.getOverridingParametersAsMap();
    }

    public String getOutput() throws JobRunException {
        if(_jobRunOutputResult != null) {
            return _jobRunOutputResult;
        }

        try {
            return JobRunHelper.getJobRunOutput(_client, RunId);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }
}
