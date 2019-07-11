package com.level11data.databricks.job.builder;

import java.util.ArrayList;
import java.util.TimeZone;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.job.JobConfigException;
import org.quartz.Trigger;

public abstract class AbstractJobBuilder implements JobBuilder {
    private String _name;
    private ArrayList<String> _emailNotificationsOnStart = new ArrayList<>();
    private ArrayList<String> _emailNotificationsOnSuccess = new ArrayList<>();
    private ArrayList<String> _emailNotificationsOnFailure = new ArrayList<>();
    private Integer _timeoutSeconds;
    private Integer _maxRetries;
    private Integer _minRetryInterval;
    private boolean _retryOnTimeout;
    private Integer _maxConcurrentRuns;
    private Trigger _scheduleTrigger;
    private TimeZone _scheduleTimeZone;

    public AbstractJobBuilder() {

    }

    public AbstractJobBuilder withName(String name) {
        _name = name;
        return this;
    }

    public AbstractJobBuilder withEmailNotificationOnStart(String email) {
        _emailNotificationsOnStart.add(email);
        return this;
    }

    public AbstractJobBuilder withEmailNotificationOnSuccess(String email) {
        _emailNotificationsOnSuccess.add(email);
        return this;
    }

    public AbstractJobBuilder withEmailNotificationOnFailure(String email) {
        _emailNotificationsOnFailure.add(email);
        return this;
    }

    public AbstractJobBuilder withTimeout(int seconds) {
        _timeoutSeconds = seconds;
        return this;
    }

    public AbstractJobBuilder withMaxRetries(int retries) {
        _maxRetries = retries;
        return this;
    }

    public AbstractJobBuilder withMinRetryInterval(int milliseconds) {
        _minRetryInterval = milliseconds;
        return this;
    }

    public AbstractJobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        _retryOnTimeout = retryOnTimeout;
        return this;
    }

    public AbstractJobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        _maxConcurrentRuns = maxConcurrentRuns;
        return this;
    }

    public AbstractJobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        _scheduleTrigger = trigger;
        _scheduleTimeZone = timeZone;
        return this;
    }

    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        jobSettingsDTO.Name = _name;
        jobSettingsDTO.TimeoutSeconds = _timeoutSeconds;
        jobSettingsDTO.MaxRetries = _maxRetries;
        jobSettingsDTO.TimeoutSeconds = _timeoutSeconds;
        jobSettingsDTO.MinRetryIntervalMillis = _minRetryInterval;
        jobSettingsDTO.RetryOnTimeout = _retryOnTimeout;
        jobSettingsDTO.MaxConcurrentRuns = _maxConcurrentRuns;

        //TODO parse cron schedule expression
        //https://stackoverflow.com/questions/3641575/how-to-get-cron-expression-given-job-name-and-group-name
        //jobSettingsDTO.Schedule = ;

        return jobSettingsDTO;
    }

    protected void validate(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //no op
    }
}
