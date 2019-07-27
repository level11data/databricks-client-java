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
    private RunState _runState;
    private final JobsClient _client;
    private final RunDTO _runDTO;
    private final long _jobId;
    private final long _runId;
    private final String _creatorUserName;
    private final long _numberInJob;
    private final long _originalAttemptRunId; //TODO convert this to a FK to the ParentJobRun
    //private final CronScheduleDTO _schedule;
    private final TriggerType _trigger;
    private final Date _startTime;
    private final List<AbstractLibrary> _libraries;

    protected AbstractJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        _client = client;
        _runDTO = runDTO;
        _jobId = runDTO.JobId;
        _runId = runDTO.RunId;
        _creatorUserName = runDTO.CreatorUserName;
        _numberInJob = runDTO.NumberInJob;
        _originalAttemptRunId = runDTO.OriginalAttemptRunId;
        _trigger = TriggerType.valueOf(runDTO.TriggerType);
        _startTime = new Date(runDTO.StartTime);

        if(runDTO.ClusterSpec.Libraries != null) {
            ArrayList<AbstractLibrary> libraries = new ArrayList<>();
            for (LibraryDTO libraryDTO : runDTO.ClusterSpec.Libraries) {
                try {
                    libraries.add(LibraryHelper.createLibrary(_client.Session.getLibrariesClient(), libraryDTO));
                } catch (LibraryConfigException e) {
                    throw new JobRunException(e);
                }
            }
            _libraries = Collections.unmodifiableList(libraries);
        } else {
            _libraries = Collections.unmodifiableList(new ArrayList<>());
        }
    }

    public RunState getRunState() throws JobRunException {
        try{
            if(_runState == null) {
                RunDTO run = _client.getRun(this._runId);
                _runState = new RunState(run.State);
            } else if(_runState.LifeCycleState.isFinal()){
                return _runState;
            } else {
                RunDTO run = _client.getRun(this._runId);
                _runState = new RunState(run.State);
            }
            return _runState;
        }catch(HttpException e){
            throw new JobRunException(e);
        }
    }

    public String getSparkContextId() throws JobRunException {
        if(_sparkContextId == null) {
            try{
                RunDTO run = _client.getRun(this._runId);
                if(run.ClusterInstance == null) {
                    return null;
                } else {
                    _sparkContextId = run.ClusterInstance.SparkContextId;
                }
            }catch(HttpException e) {
                throw new JobRunException(e);
            }
        }
        return _sparkContextId;
    }

    public Long getSetupDuration() throws JobRunException {
        if(_setupDuration == null) {
            try{
                RunDTO run = _client.getRun(this._runId);
                _setupDuration = run.SetupDuration;
            }catch(HttpException e) {
                throw new JobRunException(e);
            }
        }
        return _setupDuration;
    }

    public Long getExecutionDuration() throws JobRunException {
        if(_executionDuration == null) {
            try{
                RunDTO run = _client.getRun(this._runId);
                _executionDuration = run.ExecutionDuration;
            }catch(HttpException e) {
                throw new JobRunException(e);
            }
        }
        return _executionDuration;
    }

    public Long getCleanupDuration() throws JobRunException {
        if(_cleanupDuration == null) {
            try{
                RunDTO run = _client.getRun(this._runId);
                _cleanupDuration = run.CleanupDuration;
            }catch(HttpException e) {
                throw new JobRunException(e);
            }
        }
        return _cleanupDuration;
    }

    public void cancel() throws JobRunException {
        RunDTO runDTO = new RunDTO();
        runDTO.RunId = this._runId;

        try{
            _client.cancelRun(runDTO);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
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

    public long getJobId() {
        return _jobId;
    }

    public long getRunId() {
        return _runId;
    }

    public String getCreatorUserName() {
        return _creatorUserName;
    }

    public long getNumberInJob() {
        return _numberInJob;
    }

    //TODO convert this to a FK to the ParentJobRun
    public long getOriginalAttemptRunId() {
        return getOriginalAttemptRunId();
    }

    //public CronScheduleDTO getSchedule() { return _schedule }

    public TriggerType getTrigger() {
        return _trigger;
    }

    public Date getStartTime() {
        return _startTime;
    }

    public List<AbstractLibrary> getLibraries() {
        return _libraries;
    }

}
