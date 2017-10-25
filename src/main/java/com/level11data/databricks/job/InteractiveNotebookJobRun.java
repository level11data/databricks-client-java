package com.level11data.databricks.job;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.entities.jobs.RunDTO;

import java.util.HashMap;
import java.util.Map;

public class InteractiveNotebookJobRun extends InteractiveJobRun {
    public final String NotebookPath;
    public final Map<String,String> BaseParameters = new HashMap<String,String>();
    public final Map<String,String> OverridingParameters = new HashMap<String,String>();

    public InteractiveNotebookJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);
        if(!runDTO.isNotebookJob()) {
            throw new JobRunException("Job Run is not configured as a Notebook Job");
        }

        NotebookPath = runDTO.Task.NotebookTask.NotebookPath;

        //TODO convert array into HashMap
        //runDTO.Task.NotebookTask.BaseParameters;


        //TODO convert array into HashMap
        //runDTO.OverridingParameters.NotebookParams;
    }
}
