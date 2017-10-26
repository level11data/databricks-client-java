package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.entities.jobs.*;
import com.level11data.databricks.workspace.Notebook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InteractiveNotebookJob extends InteractiveJob {

    public final Notebook Notebook;
    public final Map<String,String> BaseParameters;

    public InteractiveNotebookJob(JobsClient client,
                                     InteractiveCluster cluster,
                                     long jobId,
                                     JobSettingsDTO jobSettingsDTO,
                                     Notebook notebook) {
        super(client, cluster, jobId, jobSettingsDTO);
        Notebook = notebook;
        BaseParameters = Collections.unmodifiableMap(new HashMap<String, String>());
    }

    public InteractiveNotebookJob(JobsClient client,
                                  InteractiveCluster cluster,
                                  long jobId,
                                  JobSettingsDTO jobSettingsDTO,
                                  Notebook notebook,
                                  Map<String,String> baseParameters) {
        super(client, cluster, jobId, jobSettingsDTO);
        Notebook = notebook;
        BaseParameters = Collections.unmodifiableMap(baseParameters);
    }

    public InteractiveNotebookJob(JobsClient client, JobDTO jobDTO)
            throws JobConfigException, ClusterConfigException, HttpException {
        super(client, client.Session.getCluster(jobDTO.Settings.ExistingClusterId), jobDTO.JobId, jobDTO.Settings);

        //Validate that the DTO represents an InteractiveNotebookJob
        JobValidation.validateInteractiveNotebookJob(jobDTO);

        Notebook notebook = new Notebook(jobDTO.Settings.NotebookTask.NotebookPath);
        Notebook = notebook;

        ParamPairDTO baseParametersDTO[] = jobDTO.Settings.NotebookTask.BaseParameters;
        if(baseParametersDTO != null) {
            HashMap<String,String> newMap = new HashMap<String,String>();
            for (int i = 0; i < baseParametersDTO.length; i++){
                newMap.put(baseParametersDTO[i].Key, baseParametersDTO[i].Value);
            }
            BaseParameters = Collections.unmodifiableMap(newMap);
        } else {
            BaseParameters = Collections.unmodifiableMap(new HashMap<String,String>());
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

        ArrayList<ParamPairDTO> paramPairList = new ArrayList<ParamPairDTO>();

        for (String key : overrideParameters.keySet()) {
            ParamPairDTO paramPairDTO = new ParamPairDTO();
            paramPairDTO.Key = key;
            paramPairDTO.Value = overrideParameters.get(key);
            paramPairList.add(paramPairDTO);
        }

        if(paramPairList.size() > 0) {
            runRequestDTO.NotebookParams = paramPairList.toArray(new ParamPairDTO[paramPairList.size()]);
        }

        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new InteractiveNotebookJobRun(_client, jobRun);
    }
}
