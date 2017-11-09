package com.level11data.databricks.job;

import com.level11data.databricks.JobsClient;
import com.level11data.databricks.entities.jobs.RunDTO;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AutomatedNotebookJobRun extends AutomatedJobRun {
    public final String NotebookPath;
    public final Map<String,String> BaseParameters;
    public final Map<String,String> OverridingParameters;

    public AutomatedNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a Notebook Job");
        }
        NotebookPath = runDTO.Task.NotebookTask.NotebookPath;

        if(runDTO.Task.NotebookTask.BaseParameters != null) {
            BaseParameters = Collections.unmodifiableMap(runDTO.Task.NotebookTask.BaseParameters);
        } else {
            BaseParameters = Collections.unmodifiableMap(new HashMap<>());
        }

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

}
