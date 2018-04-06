package com.level11data.databricks.job.builder;

import java.util.ArrayList;
import java.util.TimeZone;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import org.quartz.Trigger;

public abstract class JobBuilder {
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

    public JobBuilder() {

    }

    protected JobBuilder withName(String name) {
        _name = name;
        return this;
    }

    protected JobBuilder withEmailNotificationOnStart(String email) {
        _emailNotificationsOnStart.add(email);
        return this;
    }

    protected JobBuilder withEmailNotificationOnSuccess(String email) {
        _emailNotificationsOnSuccess.add(email);
        return this;
    }

    protected JobBuilder withEmailNotificationOnFailure(String email) {
        _emailNotificationsOnFailure.add(email);
        return this;
    }

    protected JobBuilder withTimeout(int seconds) {
        _timeoutSeconds = seconds;
        return this;
    }

    protected JobBuilder withMaxRetries(int retries) {
        _maxRetries = retries;
        return this;
    }

    protected JobBuilder withMinRetryInterval(int milliseconds) {
        _minRetryInterval = milliseconds;
        return this;
    }

    protected JobBuilder withRetryOnTimeout(boolean retryOnTimeout) {
        _retryOnTimeout = retryOnTimeout;
        return this;
    }

    protected JobBuilder withMaxConcurrentRuns(int maxConcurrentRuns) {
        _maxConcurrentRuns = maxConcurrentRuns;
        return this;
    }

    protected JobBuilder withSchedule(Trigger trigger, TimeZone timeZone) {
        _scheduleTrigger = trigger;
        _scheduleTimeZone = timeZone;
        return this;
    }

    protected JobSettingsDTO applySettings(JobSettingsDTO jobSettingsDTO) {
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

}
