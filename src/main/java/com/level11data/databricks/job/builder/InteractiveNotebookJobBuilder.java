package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.NotebookTaskDTO;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.workspace.Notebook;
import org.quartz.Trigger;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class InteractiveNotebookJobBuilder extends InteractiveJobBuilder {
    private final Notebook _notebook;
    private final JobsClient _client;
    private final Map<String,String> _baseParameters;

    public InteractiveNotebookJobBuilder(JobsClient client,
                                         InteractiveCluster cluster,
                                         Notebook notebook) {
        super(cluster);
        _client = client;
        _notebook = notebook;
        _baseParameters = new HashMap<>(); //empty map
    }

    public InteractiveNotebookJobBuilder(JobsClient client,
                                         InteractiveCluster cluster,
                                         Notebook notebook,
                                         Map<String,String> baseParameters) {
        super(cluster);
        _client = client;
        _notebook = notebook;
        _baseParameters = baseParameters;
    }

    @Override
    public InteractiveNotebookJobBuilder withName(String name) {
        return (InteractiveNotebookJobBuilder)super.withName(name);
    }

    @Override
    public InteractiveNotebookJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractiveNotebookJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public InteractiveNotebookJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractiveNotebookJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public InteractiveNotebookJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractiveNotebookJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public InteractiveNotebookJobBuilder withTimeout(int seconds) {
        return (InteractiveNotebookJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public InteractiveNotebookJobBuilder withMaxRetries(int retries) {
        return (InteractiveNotebookJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public InteractiveNotebookJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractiveNotebookJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public InteractiveNotebookJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractiveNotebookJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public InteractiveNotebookJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractiveNotebookJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public InteractiveNotebookJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractiveNotebookJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public InteractiveNotebookJobBuilder withLibrary(Library library) {
        return (InteractiveNotebookJobBuilder)super.withLibrary(library);
    }

    public InteractiveNotebookJob create() throws HttpException {
        //no validation to perform

        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        NotebookTaskDTO notebookTaskDTO = new NotebookTaskDTO();
        notebookTaskDTO.NotebookPath = _notebook.Path;
        notebookTaskDTO.BaseParameters = _baseParameters;

        jobSettingsDTO.NotebookTask = notebookTaskDTO;

        //create job via client
        long jobId = _client.createJob(jobSettingsDTO);

        //create InteractiveNotebookJob from jobSettingsDTO and jobId
        return new InteractiveNotebookJob(_client, this.Cluster, jobId, jobSettingsDTO, _notebook);
    }


}
