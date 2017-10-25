package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.entities.jobs.*;

import java.util.Date;

public abstract class Job {
    private boolean _jobInfoRequested = false;
    private JobDTO _jobDTO;

    protected JobsClient _client;

    public final long Id;
    public final String Name;
    //public final ArrayList<Library> Libraries;
    public final EmailNotification NotificationOnStart;
    public final EmailNotification NotificationOnSuccess;
    public final EmailNotification NotificationOnFailure;
    public final Integer MaxRetries;
    public final Integer MinRetryIntervalMillis;
    public final boolean RetryOnTimeout;
    //public final CronScheduleDTO Schedule;
    public final Integer MaxConcurrentRuns;
    public final Integer TimeoutSeconds;


    protected Job(JobsClient client, long jobId, JobSettingsDTO jobSettingsDTO) {
        _client = client;

        Id = jobId;
        Name = jobSettingsDTO.Name;
        MaxRetries = jobSettingsDTO.MaxRetries;
        MinRetryIntervalMillis = jobSettingsDTO.MinRetryIntervalMillis;
        RetryOnTimeout = jobSettingsDTO.RetryOnTimeout;
        MaxConcurrentRuns = jobSettingsDTO.MaxConcurrentRuns;
        TimeoutSeconds = jobSettingsDTO.TimeoutSeconds;

        //TODO Add in Libraries
        //Schedule = TODO Add in Schedule

        NotificationOnStart = initOnStartNotification(jobSettingsDTO.EmailNotifications);
        NotificationOnSuccess = initOnSuccessNotification(jobSettingsDTO.EmailNotifications);
        NotificationOnFailure = initOnFailureNotification(jobSettingsDTO.EmailNotifications);
    }

    protected Job(JobsClient client, JobDTO jobDTO) {
        _client = client;
        _jobDTO = jobDTO;
        _jobInfoRequested = true;

        Id = jobDTO.JobId;
        Name = jobDTO.Settings.Name;
        MaxRetries = jobDTO.Settings.MaxRetries;
        MinRetryIntervalMillis = jobDTO.Settings.MinRetryIntervalMillis;
        RetryOnTimeout = jobDTO.Settings.RetryOnTimeout;
        MaxConcurrentRuns = jobDTO.Settings.MaxConcurrentRuns;
        TimeoutSeconds = jobDTO.Settings.TimeoutSeconds;

        //TODO Add in Libraries
        //Schedule = TODO Add in Schedule

        NotificationOnStart = initOnStartNotification(jobDTO.Settings.EmailNotifications);
        NotificationOnSuccess = initOnSuccessNotification(jobDTO.Settings.EmailNotifications);
        NotificationOnFailure = initOnFailureNotification(jobDTO.Settings.EmailNotifications);
    }


    private EmailNotification initOnStartNotification(JobEmailNotificationsDTO emailNotifications) {
        if(emailNotifications != null) {
            if(emailNotifications.OnStart != null) {
                return new EmailNotification(emailNotifications.OnStart);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private EmailNotification initOnSuccessNotification(JobEmailNotificationsDTO emailNotifications) {
        if(emailNotifications != null) {
            if(emailNotifications.OnSuccess != null) {
                return new EmailNotification(emailNotifications.OnSuccess);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private EmailNotification initOnFailureNotification(JobEmailNotificationsDTO emailNotifications) {
        if(emailNotifications != null) {
            if(emailNotifications.OnFailure != null) {
                return new EmailNotification(emailNotifications.OnFailure);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private void initJobInfo() throws HttpException {
        if(!_jobInfoRequested) {
            _jobDTO = _client.getJob(Id);
            _jobInfoRequested = true;
        }
    }

    public Date getCreatedTime() throws HttpException {
        if(!_jobInfoRequested) {
            initJobInfo();
        }
        return new Date(_jobDTO.CreatedTime);
    }

    public String getCreatorUserName() throws HttpException {
        if(!_jobInfoRequested) {
            initJobInfo();
        }
        return _jobDTO.CreatorUserName;
    }

    public void delete() throws HttpException {
        _client.deleteJob(this.Id);
    }
}
