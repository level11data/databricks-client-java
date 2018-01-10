package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.client.entities.jobs.JobEmailNotificationsDTO;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.library.*;
import com.level11data.databricks.library.util.LibraryHelper;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public abstract class Job {
    private boolean _jobInfoRequested = false;
    private JobDTO _jobDTO;
    private LibrariesClient _librariesClient;
    private JobsClient _client;

    public final long Id;
    public final String Name;
    public final List<Library> Libraries;
    public final EmailNotification NotificationOnStart;
    public final EmailNotification NotificationOnSuccess;
    public final EmailNotification NotificationOnFailure;
    public final Integer MaxRetries;
    public final Integer MinRetryIntervalMillis;
    public final boolean RetryOnTimeout;
    //public final CronScheduleDTO Schedule;
    public final Integer MaxConcurrentRuns;
    public final Integer TimeoutSeconds;

    protected Job(JobsClient client, long jobId, JobSettingsDTO jobSettingsDTO) throws LibraryConfigException, URISyntaxException {
        _client = client;

        Id = jobId;
        Name = jobSettingsDTO.Name;
        MaxRetries = jobSettingsDTO.MaxRetries;
        MinRetryIntervalMillis = jobSettingsDTO.MinRetryIntervalMillis;
        RetryOnTimeout = jobSettingsDTO.RetryOnTimeout;
        MaxConcurrentRuns = jobSettingsDTO.MaxConcurrentRuns;
        TimeoutSeconds = jobSettingsDTO.TimeoutSeconds;

        List<Library> libraryList = new ArrayList<Library>();
        if(jobSettingsDTO.Libraries != null) {
            for (LibraryDTO libraryDTO : jobSettingsDTO.Libraries) {
                libraryList.add(LibraryHelper.createLibrary(getLibrariesClient(), libraryDTO));
            }
        }
        Libraries = Collections.unmodifiableList(libraryList);

        //Schedule = TODO Add in Schedule

        NotificationOnStart = initOnStartNotification(jobSettingsDTO.EmailNotifications);
        NotificationOnSuccess = initOnSuccessNotification(jobSettingsDTO.EmailNotifications);
        NotificationOnFailure = initOnFailureNotification(jobSettingsDTO.EmailNotifications);
    }

    private boolean isLibraryDtoInLibraries(LibraryDTO libraryDTO) {
        for (Library library : Libraries) {
            if(library.equals(libraryDTO)) {
                return true;
            }
        }
        return false;
    }

    protected Job(JobsClient client, JobDTO jobDTO)
            throws LibraryConfigException, URISyntaxException {
        this(client, jobDTO.JobId, jobDTO.Settings);
    }

    protected Job(JobsClient client, JobDTO jobDTO, List<Library> libraries)
            throws LibraryConfigException, URISyntaxException {
        this(client, jobDTO.JobId, jobDTO.Settings);
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

    private LibrariesClient getLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient = new LibrariesClient(_client.Session);
        }
        return _librariesClient;
    }
}
