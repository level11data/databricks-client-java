package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.entities.jobs.ParamPairDTO;
import com.level11data.databricks.entities.jobs.RunDTO;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InteractiveNotebookJobRun extends InteractiveJobRun {
    public final String NotebookPath;
    public final Map<String,String> BaseParameters;
    public final Map<String,String> OverridingParameters;

    public InteractiveNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a Notebook Job");
        }
        NotebookPath = runDTO.Task.NotebookTask.NotebookPath;

        HashMap<String,String> newBaseMap = new HashMap<String,String>();

        if(runDTO.Task.NotebookTask.BaseParameters != null) {
            for (ParamPairDTO parameterDTO : runDTO.Task.NotebookTask.BaseParameters) {
                newBaseMap.put(parameterDTO.Key, parameterDTO.Value);
            }
        }
        BaseParameters = Collections.unmodifiableMap(newBaseMap);

        HashMap<String,String> newOverrideMap = new HashMap<String,String>();

        if(runDTO.OverridingParameters != null) {
            if(runDTO.OverridingParameters.NotebookParams != null) {
                for (ParamPairDTO parameterDTO : runDTO.OverridingParameters.NotebookParams) {
                    newOverrideMap.put(parameterDTO.Key, parameterDTO.Value);
                }
            }
        }
        OverridingParameters = Collections.unmodifiableMap(newOverrideMap);
    }
}
