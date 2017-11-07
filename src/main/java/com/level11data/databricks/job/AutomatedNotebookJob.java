package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.entities.jobs.*;
import com.level11data.databricks.workspace.Notebook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AutomatedNotebookJob extends AutomatedJob {

    public final com.level11data.databricks.workspace.Notebook Notebook;
    public final Map<String,String> BaseParameters;

    /**
     * Create a Notebook Job on an Automated Cluster with NO parameters
     *
     * @param client
     * @param jobId
     * @param jobSettingsDTO
     * @param notebook
     */
    public AutomatedNotebookJob(JobsClient client,
                                  long jobId,
                                  JobSettingsDTO jobSettingsDTO,
                                  Notebook notebook) {
        super(client, jobId, jobSettingsDTO);
        Notebook = notebook;
        BaseParameters = Collections.unmodifiableMap(new HashMap<String, String>());
    }

    /**
     * Create a Notebook Job on an Interactive Cluster WITH parameters
     *
     * @param client
     * @param jobId
     * @param jobSettingsDTO
     * @param notebook
     * @param baseParameters
     */
    public AutomatedNotebookJob(JobsClient client,
                                  long jobId,
                                  JobSettingsDTO jobSettingsDTO,
                                  Notebook notebook,
                                  Map<String,String> baseParameters) {
        super(client, jobId, jobSettingsDTO);
        Notebook = notebook;
        BaseParameters = Collections.unmodifiableMap(baseParameters);
    }

    /**
     * Create a Notebook Job on an Interactive Cluster using a Job DTO object
     *
     * @param client
     * @param jobDTO
     * @throws JobConfigException
     * @throws ClusterConfigException
     * @throws HttpException
     */
    public AutomatedNotebookJob(JobsClient client, JobDTO jobDTO)
            throws JobConfigException, ClusterConfigException, HttpException {
        super(client, jobDTO.JobId, jobDTO.Settings);

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

    public AutomatedNotebookJobRun run() throws HttpException, JobRunException {
        //simple run request with no parameter overrides
        RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
        runRequestDTO.JobId = this.Id;
        RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
        RunDTO jobRun = _client.getRun(response.RunId);
        return new AutomatedNotebookJobRun(_client, jobRun);
    }

    public AutomatedNotebookJobRun run(Map<String,String> overrideParameters) throws HttpException, JobRunException {
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
        return new AutomatedNotebookJobRun(_client, jobRun);
    }

}
