package com.level11data.databricks.job.run;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.job.TriggerType;
import com.level11data.databricks.library.AbstractLibrary;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.library.util.LibraryHelper;
import java.util.*;

abstract public class AbstractJobRun implements JobRun {
    private String _sparkContextId;
    private Long _setupDuration;
    private Long _executionDuration;
    private Long _cleanupDuration;
    private final JobsClient _client;
    private final RunDTO _runDTO;

    public final long JobId;
    public final long RunId;
    public final String CreatorUserName;
    public final long NumberInJob;
    public final long OriginalAttemptRunId; //TODO convert this to a FK to the ParentJobRun
    //public final CronScheduleDTO Schedule;
    public final TriggerType Trigger;
    public final Date StartTime;
    public final List<AbstractLibrary> Libraries;

    protected AbstractJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        _client = client;
        _runDTO = runDTO;
        JobId = runDTO.JobId;
        RunId = runDTO.RunId;
        CreatorUserName = runDTO.CreatorUserName;
        NumberInJob = runDTO.NumberInJob;
        OriginalAttemptRunId = runDTO.OriginalAttemptRunId;
        Trigger = TriggerType.valueOf(runDTO.TriggerType);
        StartTime = new Date(runDTO.StartTime);

        if(runDTO.ClusterSpec.Libraries != null) {
            ArrayList<AbstractLibrary> libraries = new ArrayList<>();
            for (LibraryDTO libraryDTO : runDTO.ClusterSpec.Libraries) {
                try {
                    libraries.add(LibraryHelper.createLibrary(_client.Session.getLibrariesClient(), libraryDTO));
                } catch (LibraryConfigException e) {
                    throw new JobRunException(e);
                }
            }
            Libraries = Collections.unmodifiableList(libraries);
        } else {
            Libraries = Collections.unmodifiableList(new ArrayList<>());
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

    private String[] getBaseParametersFromDTOAsArray() {
        if(this instanceof AutomatedPythonJobRun) {
            return _runDTO.Task.SparkPythonTask.Parameters;
        } else if(this instanceof InteractivePythonJobRun) {
            return _runDTO.Task.SparkPythonTask.Parameters;
        } else if(this instanceof AutomatedJarJobRun) {
            return _runDTO.Task.SparkJarTask.Parameters;
        } else if(this instanceof InteractiveJarJobRun) {
            return _runDTO.Task.SparkJarTask.Parameters;
        } else {
            return null;  //TODO include remaining AbstractJobRun types
        }
    }

    private String[] getOverridingParametersFromDTOAsArray() {
        if(_runDTO.OverridingParameters == null) return null;

        if(this instanceof AutomatedPythonJobRun) {
            return _runDTO.OverridingParameters.PythonParams;
        } else if(this instanceof InteractivePythonJobRun) {
            return _runDTO.OverridingParameters.PythonParams;
        } else if(this instanceof AutomatedJarJobRun) {
            return _runDTO.OverridingParameters.JarParams;
        } else if(this instanceof InteractiveJarJobRun) {
            return _runDTO.OverridingParameters.JarParams;
        } else {
            return null;  //TODO include remaining AbstractJobRun types
        }
    }

    private Map<String,String> getBaseParametersFromDTOAsMap() {
        if(this instanceof AutomatedNotebookJobRun) {
            return _runDTO.Task.NotebookTask.BaseParameters;
        } else if(this instanceof InteractiveNotebookJobRun) {
            return _runDTO.Task.NotebookTask.BaseParameters;
        } else {
            return null;
        }
    }

    private Map<String,String> getOverridingParametersFromDTOAsMap() {
        if(_runDTO.OverridingParameters == null) return null;

        if(this instanceof AutomatedNotebookJobRun) {
            return _runDTO.OverridingParameters.NotebookParams;
        } else if(this instanceof InteractiveNotebookJobRun) {
            return _runDTO.OverridingParameters.NotebookParams;
        } else {
            return null;
        }
    }

    protected List<String> getBaseParametersAsList() {
        if(getBaseParametersFromDTOAsArray() != null) {
            ArrayList<String> params = new ArrayList<>();
            Collections.addAll(params, getBaseParametersFromDTOAsArray());
            return Collections.unmodifiableList(params);
        } else {
            return Collections.unmodifiableList(new ArrayList<>());
        }
    }

    protected List<String> getOverridingParametersAsList() {
        if(getOverridingParametersFromDTOAsArray() != null) {
            ArrayList<String> params = new ArrayList<>();
            Collections.addAll(params, getOverridingParametersFromDTOAsArray());
            return Collections.unmodifiableList(params);
        } else {
            return Collections.unmodifiableList(new ArrayList<>());
        }
    }

    protected Map<String, String> getBaseParametersAsMap() {
        if (getBaseParametersFromDTOAsMap() != null) {
            return Collections.unmodifiableMap(getBaseParametersFromDTOAsMap());
        } else {
            return Collections.unmodifiableMap(new HashMap<>());
        }
    }

    protected Map<String, String> getOverridingParametersAsMap() {
        if (getOverridingParametersFromDTOAsMap() != null) {
            return Collections.unmodifiableMap(getOverridingParametersFromDTOAsMap());
        } else {
            return Collections.unmodifiableMap(new HashMap<>());
        }
    }
}
