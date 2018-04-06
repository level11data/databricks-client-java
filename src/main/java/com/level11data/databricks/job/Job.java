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

    protected Job(JobsClient client, JobDTO jobDTO, List<Library> libraries) throws JobConfigException {
        this(client, new Long(jobDTO.JobId), jobDTO.Settings, libraries);
    }

    protected Job(JobsClient client, JobDTO jobDTO) throws JobConfigException {
        this(client, new Long(jobDTO.JobId), jobDTO.Settings, null);
    }

    protected Job(JobsClient client, Long jobId, JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        this(client, jobId, jobSettingsDTO, null);
    }

    protected Job(JobsClient client, Long jobId, JobSettingsDTO jobSettingsDTO, List<Library> libraries) throws JobConfigException {
        _client = client;

        try {
            if(jobId == null) {
                Id = client.createJob(jobSettingsDTO);
            } else {
                Id = jobId.longValue();
            }
        } catch(HttpException e) {
            throw new JobConfigException(e);
        }

        Name = jobSettingsDTO.Name;
        MaxRetries = jobSettingsDTO.MaxRetries;
        MinRetryIntervalMillis = jobSettingsDTO.MinRetryIntervalMillis;
        RetryOnTimeout = jobSettingsDTO.RetryOnTimeout;
        MaxConcurrentRuns = jobSettingsDTO.MaxConcurrentRuns;
        TimeoutSeconds = jobSettingsDTO.TimeoutSeconds;

        try {
            List<Library> libraryList = libraries == null ? new ArrayList<Library>() : libraries;
            if(jobSettingsDTO.Libraries != null) {
                for (LibraryDTO libraryDTO : jobSettingsDTO.Libraries) {
                    //maintain object reference for passed in Libraries
                    if(!isInList(libraryDTO, libraryList)) {
                        libraryList.add(LibraryHelper.createLibrary(getLibrariesClient(), libraryDTO));
                    }
                }
            }
            Libraries = Collections.unmodifiableList(libraryList);
        } catch(LibraryConfigException e) {
            throw new JobConfigException(e);
        }

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

    public Date getCreatedTime() throws JobConfigException {
        try{
            if(!_jobInfoRequested) {
                initJobInfo();
            }
            return new Date(_jobDTO.CreatedTime);
        } catch(HttpException e) {
            throw new JobConfigException(e);
        }
    }

    public String getCreatorUserName() throws JobConfigException {
        try {
            if(!_jobInfoRequested) {
                initJobInfo();
            }
            return _jobDTO.CreatorUserName;
        } catch(HttpException e) {
            throw new JobConfigException(e);
        }
    }

    public void delete() throws JobConfigException {
        try {
            _client.deleteJob(this.Id);
        } catch(HttpException e) {
            throw new JobConfigException(e);
        }
    }

    private LibrariesClient getLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient = new LibrariesClient(_client.Session);
        }
        return _librariesClient;
    }

    private boolean isInList(LibraryDTO libraryDTO, List<Library> libraries) {
        for (Library library : libraries) {
            if(library.equals(libraryDTO)) {
                return true;
            }
        }
        //not in list
        return false;
    }
}
