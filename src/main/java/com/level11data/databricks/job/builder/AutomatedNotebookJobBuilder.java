package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;
import com.level11data.databricks.entities.jobs.*;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.workspace.Notebook;
import org.quartz.Trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class AutomatedNotebookJobBuilder extends AutomatedJobBuilder {
    private AutomatedClusterBuilder _clusterBuilder ;
    private final Notebook _notebook;
    private final JobsClient _client;
    private final Map<String,String> _baseParameters;


    public AutomatedNotebookJobBuilder(JobsClient client, Notebook notebook) {
        super();
        _client = client;
        _notebook = notebook;
        _baseParameters = new HashMap<String,String>(); //empty map
    }

    public AutomatedNotebookJobBuilder(JobsClient client, Notebook notebook, Map<String,String> baseParameters) {
        super();
        _client = client;
        _notebook = notebook;
        _baseParameters = baseParameters;
    }

    @Override
    public AutomatedNotebookJobBuilder withName(String name) {
        return (AutomatedNotebookJobBuilder)super.withName(name);
    }

    @Override
    public AutomatedNotebookJobBuilder withEmailNotificationOnStart(String email) {
        return (AutomatedNotebookJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public AutomatedNotebookJobBuilder withEmailNotificationOnSuccess(String email) {
        return (AutomatedNotebookJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public AutomatedNotebookJobBuilder withEmailNotificationOnFailure(String email) {
        return (AutomatedNotebookJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public AutomatedNotebookJobBuilder withTimeout(int seconds) {
        return (AutomatedNotebookJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public AutomatedNotebookJobBuilder withMaxRetries(int retries) {
        return (AutomatedNotebookJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public AutomatedNotebookJobBuilder withMinRetryInterval(int milliseconds) {
        return (AutomatedNotebookJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public AutomatedNotebookJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (AutomatedNotebookJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public AutomatedNotebookJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (AutomatedNotebookJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public AutomatedNotebookJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (AutomatedNotebookJobBuilder)super.withSchedule(trigger, timeZone);
    }

    public AutomatedNotebookJob create() throws HttpException {
        //no validation to perform
        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        NotebookTaskDTO notebookTaskDTO = new NotebookTaskDTO();
        notebookTaskDTO.NotebookPath = _notebook.Path;

        if(_baseParameters.size() > 0 ){
            ArrayList<ParamPairDTO> paramPairs = new ArrayList<ParamPairDTO>();
            for(String key : _baseParameters.keySet()) {
                String value = _baseParameters.get(key);
                ParamPairDTO paramPairDTO = new ParamPairDTO();
                paramPairDTO.Key = key;
                paramPairDTO.Value = value;
                paramPairs.add(paramPairDTO);
            }
            notebookTaskDTO.BaseParameters = paramPairs.toArray(new ParamPairDTO[paramPairs.size()]);
        }
        jobSettingsDTO.NotebookTask = notebookTaskDTO;

        //create job via client
        long jobId = _client.createJob(jobSettingsDTO);

        //create InteractiveNotebookJob from jobSettingsDTO and jobId
        return new AutomatedNotebookJob(_client, jobId, jobSettingsDTO, _notebook);
    }

    public AutomatedClusterBuilder withClusterSpec(int numWorkers) {
        if (_clusterBuilder == null) {
            _clusterBuilder = new AutomatedClusterBuilder(this, numWorkers);
        }
        return _clusterBuilder;
    }

}
