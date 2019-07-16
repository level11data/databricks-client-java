package com.level11data.databricks.job.builder;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.SparkSubmitTaskDTO;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.job.AutomatedSparkSubmitJob;
import com.level11data.databricks.job.JobConfigException;
import org.quartz.Trigger;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class AutomatedSparkSubmitJobBuilder extends AbstractAutomatedJobBuilder {

    private final JobsClient _client;
    private List<String> _baseParameters;

    public AutomatedSparkSubmitJobBuilder(JobsClient client,
                                          List<String> parameters) {
        super();
        _client = client;

        if(parameters != null) {
            _baseParameters = parameters;
        } else {
            _baseParameters = new ArrayList<String>();
        }
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withName(String name) {
        return (AutomatedSparkSubmitJobBuilder)super.withName(name);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withEmailNotificationOnStart(String email) {
        return (AutomatedSparkSubmitJobBuilder)super.withEmailNotificationOnStart(email);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withEmailNotificationOnSuccess(String email) {
        return (AutomatedSparkSubmitJobBuilder)super.withEmailNotificationOnSuccess(email);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withEmailNotificationOnFailure(String email) {
        return (AutomatedSparkSubmitJobBuilder)super.withEmailNotificationOnFailure(email);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withTimeout(int seconds) {
        return (AutomatedSparkSubmitJobBuilder)super.withTimeout(seconds);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withMaxRetries(int retries) {
        return (AutomatedSparkSubmitJobBuilder)super.withMaxRetries(retries);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withMinRetryInterval(int milliseconds) {
        return (AutomatedSparkSubmitJobBuilder)super.withMinRetryInterval(milliseconds);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        return (AutomatedSparkSubmitJobBuilder)super.withRetryOnTimeout(retryOnTimeout);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        return (AutomatedSparkSubmitJobBuilder)super.withMaxConcurrentRuns(maxConcurrentRuns);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        return (AutomatedSparkSubmitJobBuilder)super.withSchedule(trigger, timeZone);
    }

    @Override
    public AutomatedSparkSubmitJobBuilder withClusterSpec(ClusterSpec clusterSpec) {
        return (AutomatedSparkSubmitJobBuilder)super.withClusterSpec(clusterSpec);
    }

    public AutomatedSparkSubmitJobBuilder withBaseParameter(String parameter) {
        _baseParameters.add(parameter);
        return this;
    }

    @Override
    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super.validate(jobSettingsDTO);
    }

    public AutomatedSparkSubmitJob create() throws JobConfigException {
        JobSettingsDTO jobSettingsDTO = new JobSettingsDTO();
        jobSettingsDTO = super.applySettings(jobSettingsDTO);

        SparkSubmitTaskDTO sparkSubmitTaskDTO = new SparkSubmitTaskDTO();

        if(_baseParameters != null) {
            sparkSubmitTaskDTO.Parameters = _baseParameters.toArray(new String[_baseParameters.size()]);
        }

        jobSettingsDTO.SparkSubmitTask = sparkSubmitTaskDTO;

        validate(jobSettingsDTO);

        return new AutomatedSparkSubmitJob(_client, jobSettingsDTO);
    }
}
