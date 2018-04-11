package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.util.JobRunHelper;
import java.util.Map;

public class InteractiveNotebookJobRun extends InteractiveJobRun implements INotebookJobRun {
    private JobsClient _client;
    private String _jobRunOutputResult;

    public final String NotebookPath;
    public final Map<String,String> BaseParameters;
    public final Map<String,String> OverridingParameters;

    public InteractiveNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        _client = client;
        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a Notebook Job");
        }
        NotebookPath = runDTO.Task.NotebookTask.NotebookPath;

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
