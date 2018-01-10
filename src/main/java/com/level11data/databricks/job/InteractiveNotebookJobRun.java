package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.util.JobRunHelper;
import com.level11data.databricks.library.LibraryConfigException;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InteractiveNotebookJobRun extends InteractiveJobRun {
    private JobsClient _client;
    private String _jobRunOutputResult;

    public final String NotebookPath;
    public final Map<String,String> BaseParameters;
    public final Map<String,String> OverridingParameters;

    public InteractiveNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException, LibraryConfigException, URISyntaxException {
        super(client, runDTO);
        _client = client;
        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a Notebook Job");
        }
        NotebookPath = runDTO.Task.NotebookTask.NotebookPath;

        if(runDTO.Task.NotebookTask.BaseParameters != null) {
            BaseParameters = Collections.unmodifiableMap(runDTO.Task.NotebookTask.BaseParameters);
        } else {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        }
        HashMap<String,String> newOverrideMap = new HashMap<String,String>();

        if(runDTO.OverridingParameters != null) {
            if(runDTO.OverridingParameters.NotebookParams != null) {
                OverridingParameters = Collections.unmodifiableMap(runDTO.OverridingParameters.NotebookParams);
            } else {
                OverridingParameters = Collections.unmodifiableMap(new HashMap<>());
            }
        } else {
            OverridingParameters = Collections.unmodifiableMap(new HashMap<>());
        }
    }

    public String getOutput() throws HttpException, JobRunException {
        if(_jobRunOutputResult != null) {
            return _jobRunOutputResult;
        }

        return JobRunHelper.getJobRunOutput(_client, RunId);
    }
}
