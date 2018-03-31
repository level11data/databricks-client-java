package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.job.TriggerType;
import com.level11data.databricks.library.Library;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.library.util.LibraryHelper;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

abstract public class JobRun {
    private String _sparkContextId;
    private Long _setupDuration;
    private Long _executionDuration;
    private Long _cleanupDuration;
    private JobsClient _client;

    public final long JobId;
    public final long RunId;
    public final String CreatorUserName;
    public final long NumberInJob;
    public final long OriginalAttemptRunId; //TODO convert this to a FK to the ParentJobRun
    //public final CronScheduleDTO Schedule;
    public final TriggerType Trigger;
    public final Date StartTime;
    public final List<Library> Libraries;

    protected JobRun(JobsClient client, RunDTO runDTO) throws LibraryConfigException, URISyntaxException {
        _client = client;
        JobId = runDTO.JobId;
        RunId = runDTO.RunId;
        CreatorUserName = runDTO.CreatorUserName;
        NumberInJob = runDTO.NumberInJob;
        OriginalAttemptRunId = runDTO.OriginalAttemptRunId;
        Trigger = TriggerType.valueOf(runDTO.TriggerType);
        StartTime = new Date(runDTO.StartTime);

        if(runDTO.ClusterSpec.Libraries != null) {
            ArrayList<Library> libraries = new ArrayList<>();
            for (LibraryDTO libraryDTO : runDTO.ClusterSpec.Libraries) {
                libraries.add(LibraryHelper.createLibrary(_client.Session.getLibrariesClient(), libraryDTO));
            }
            Libraries = Collections.unmodifiableList(libraries);
        } else {
            Libraries = Collections.unmodifiableList(new ArrayList<Library>());
        }

    }

    //TODO Be more clever with not making an API call if the RunState is in a final state
    public RunState getRunState() throws HttpException {
        RunDTO run = _client.getRun(this.RunId);
        return new RunState(run.State);
    }

    public String getSparkContextId() throws HttpException {
        if(_sparkContextId == null) {
            RunDTO run = _client.getRun(this.RunId);
            if(run.ClusterInstance == null) {
                return null;
            } else {
                _sparkContextId = run.ClusterInstance.SparkContextId;
            }
        }
        return _sparkContextId;
    }

    public Long getSetupDuration() throws HttpException {
        if(_setupDuration == null) {
            RunDTO run = _client.getRun(this.RunId);
            _setupDuration = run.SetupDuration;
        }
        return _setupDuration;
    }

    public Long getExecutionDuration() throws HttpException {
        if(_executionDuration == null) {
            RunDTO run = _client.getRun(this.RunId);
            _executionDuration = run.ExecutionDuration;
        }
        return _executionDuration;
    }

    public Long getCleanupDuration() throws HttpException {
        if(_cleanupDuration == null) {
            RunDTO run = _client.getRun(this.RunId);
            _cleanupDuration = run.CleanupDuration;
        }
        return _cleanupDuration;
    }
}
