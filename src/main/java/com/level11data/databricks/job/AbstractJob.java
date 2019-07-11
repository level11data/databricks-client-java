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

public abstract class AbstractJob {
    private boolean _jobInfoRequested = false;
    private JobDTO _jobDTO;
    private LibrariesClient _librariesClient;
    private JobsClient _client;

    private final long _id;
    private final String _name;
    private final List<Library> _libraries;
    private final EmailNotification _notificationOnStart;
    private final EmailNotification _notificationOnSuccess;
    private final EmailNotification _notificationOnFailure;
    private final Integer _maxRetries;
    private final Integer _minRetryIntervalMillis;
    private final boolean _retryOnTimeout;
    //private final CronScheduleDTO _schedule;
    private final Integer _maxConcurrentRuns;
    private final Integer _timeoutSeconds;

    protected AbstractJob(JobsClient client, JobDTO jobDTO, List<Library> libraries) throws JobConfigException {
        this(client, new Long(jobDTO.JobId), jobDTO.Settings, libraries);
    }

    protected AbstractJob(JobsClient client, JobDTO jobDTO) throws JobConfigException {
        this(client, new Long(jobDTO.JobId), jobDTO.Settings, null);
    }

    protected AbstractJob(JobsClient client, Long jobId, JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        this(client, jobId, jobSettingsDTO, null);
    }

    protected AbstractJob(JobsClient client, Long jobId, JobSettingsDTO jobSettingsDTO, List<Library> libraries) throws JobConfigException {
        _client = client;

        try {
            if(jobId == null) {
                _id = client.createJob(jobSettingsDTO);
            } else {
                _id = jobId.longValue();
            }
        } catch(HttpException e) {
            throw new JobConfigException(e);
        }

        _name = jobSettingsDTO.Name;
        _maxRetries = jobSettingsDTO.MaxRetries;
        _minRetryIntervalMillis = jobSettingsDTO.MinRetryIntervalMillis;
        _retryOnTimeout = jobSettingsDTO.RetryOnTimeout;
        _maxConcurrentRuns = jobSettingsDTO.MaxConcurrentRuns;
        _timeoutSeconds = jobSettingsDTO.TimeoutSeconds;

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
            _libraries = Collections.unmodifiableList(libraryList);
        } catch(LibraryConfigException e) {
            throw new JobConfigException(e);
        }

        //_schedule = TODO Add in Schedule

        _notificationOnStart = initOnStartNotification(jobSettingsDTO.EmailNotifications);
        _notificationOnSuccess = initOnSuccessNotification(jobSettingsDTO.EmailNotifications);
        _notificationOnFailure = initOnFailureNotification(jobSettingsDTO.EmailNotifications);
    }

    private boolean isLibraryDtoInLibraries(LibraryDTO libraryDTO) {
        for (Library library : _libraries) {
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
            _jobDTO = _client.getJob(_id);
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
        JobDTO jobDTO = new JobDTO();
        jobDTO.JobId = this._id;
            _client.deleteJob(jobDTO);
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

    public long getId(){
        return _id;
    }

    public String getName(){
        return _name;
    }

    public List<Library> getLibraries(){
        return _libraries;
    }

    public EmailNotification getNotificationOnStart() {
        return _notificationOnStart;
    }

    public EmailNotification getNotificationOnSuccess() {
        return _notificationOnSuccess;
    }

    public EmailNotification getNotificationOnFailure() {
        return _notificationOnFailure;
    }

    public Integer getMaxRetries() {
        return _maxRetries;
    }

    public Integer getMinRetryIntervalMillis() {
        return _minRetryIntervalMillis;
    }

    public boolean getRetryOnTimeout() {
        return _retryOnTimeout;
    }

    //public CronScheduleDTO getSchedule() { return _schedule; }

    public Integer getMaxConcurrentRuns() {
        return _maxConcurrentRuns;
    }

    public Integer getTimeoutSeconds() {
        return _timeoutSeconds;
    }

}
