package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.entities.jobs.NotebookTaskDTO;
import com.level11data.databricks.entities.jobs.ParamPairDTO;
import com.level11data.databricks.entities.jobs.SparkJarTaskDTO;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.workspace.Notebook;
import org.quartz.Trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class InteractiveJarJobBuilder extends InteractiveJobBuilder {
    private final JobsClient _client;
    private final String _mainClassName;
    //private final JarLibrary _jar; //TODO Set JarLibrary
    private final ArrayList<String> _baseParameters;


    public InteractiveJarJobBuilder(JobsClient client,
                                    InteractiveCluster cluster,
                                    String mainClassName) {
        super(cluster);
        _client = client;
        _mainClassName = mainClassName;
        _baseParameters = new ArrayList<String>(); //empty arraylist
    }

    public InteractiveJarJobBuilder(JobsClient client,
                                    InteractiveCluster cluster,
                                    String mainClassName,
                                    ArrayList<String> baseParameters) {
        super(cluster);
        _client = client;
        _mainClassName = mainClassName;
        _baseParameters = baseParameters;
    }

    @Override
    public InteractiveJarJobBuilder withName(String name) {
        return (InteractiveJarJobBuilder)super.withName(name);
    }

    @Override
    public InteractiveJarJobBuilder withEmailNotificationOnStart(String email) {
        return (InteractiveJarJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public InteractiveJarJobBuilder withEmailNotificationOnSuccess(String email) {
        return (InteractiveJarJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public InteractiveJarJobBuilder withEmailNotificationOnFailure(String email) {
        return (InteractiveJarJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public InteractiveJarJobBuilder withTimeout(int seconds) {
        return (InteractiveJarJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public InteractiveJarJobBuilder withMaxRetries(int retries) {
        return (InteractiveJarJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public InteractiveJarJobBuilder withMinRetryInterval(int milliseconds) {
        return (InteractiveJarJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public InteractiveJarJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (InteractiveJarJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public InteractiveJarJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (InteractiveJarJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public InteractiveJarJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (InteractiveJarJobBuilder)super.withSchedule(trigger, timeZone);
    }

    /**
    public InteractiveJarJob create() throws HttpException {
        //no validation to perform

        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        jobSettingsDTO = super.applySettings(jobSettingsDTO);


        SparkJarTaskDTO jarTaskDTO = new SparkJarTaskDTO();
        jarTaskDTO.JarUri = ;
        jarTaskDTO.MainClassName = _mainClassName;
        jarTaskDTO.Parameters = ;

        jobSettingsDTO.SparkJarTask = jarTaskDTO;


        //create job via client
        long jobId = _client.createJob(jobSettingsDTO);

        //create InteractiveNotebookJob from jobSettingsDTO and jobId
        return new InteractiveJarJob(_client, this.Cluster, jobId, jobSettingsDTO, _mainClass);
    }
    **/


}
