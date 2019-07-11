package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.*;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.job.run.InteractivePythonJobRun;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.library.Library;

import java.util.List;

public class InteractivePythonJob extends AbstractInteractiveJob implements StandardJob {
    private final JobsClient _client;
    private final String[] _parameters;
    private final PythonScript _pythonScript;


    public InteractivePythonJob(JobsClient client,
                                InteractiveCluster cluster,
                                PythonScript pythonScript,
                                JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        this(client, cluster, null, jobSettingsDTO, null);
    }

    public InteractivePythonJob(JobsClient client,
                                InteractiveCluster cluster,
                                PythonScript pythonScript,
                                JobSettingsDTO jobSettingsDTO,
                                List<Library> libraries) throws JobConfigException {
        super(client, cluster, null, jobSettingsDTO, libraries);
        _client = client;
        _pythonScript = pythonScript;

        //Validate DTO for this Job Type
        JobValidation.validateInteractivePythonJob(jobSettingsDTO);

        _parameters = jobSettingsDTO.SparkPythonTask.Parameters;
    }

    public InteractivePythonJob(JobsClient client,
                                InteractiveCluster cluster,
                                PythonScript pythonScript,
                                JobDTO jobDTO) throws JobConfigException {
        this(client, cluster, pythonScript, jobDTO, null);
    }

    public InteractivePythonJob(JobsClient client,
                                InteractiveCluster cluster,
                                PythonScript pythonScript,
                                JobDTO jobDTO,
                                List<Library> libraries) throws JobConfigException {
        super(client, cluster, jobDTO.JobId, jobDTO.Settings, libraries);
        _client = client;
        _pythonScript = pythonScript;

        //Validate DTO for this Job Type
        JobValidation.validateInteractivePythonJob(jobDTO);

        _parameters = jobDTO.Settings.SparkPythonTask.Parameters;
    }

    public InteractivePythonJobRun run() throws JobRunException {
        return run(null);
    }

    public InteractivePythonJobRun run(List<String> overrideParameters) throws JobRunException {
        try {
            RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
            runRequestDTO.JobId = this.getId();

            if(overrideParameters != null) {
                runRequestDTO.PythonParams = overrideParameters.toArray(new String[overrideParameters.size()]);
            }
            RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
            RunDTO jobRun = _client.getRun(response.RunId);
            return new InteractivePythonJobRun(_client, _pythonScript, jobRun);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }

    public String[] getParameters() {
        return _parameters;
    }

    public PythonScript getPythonScript() {
        return _pythonScript;
    }
}
