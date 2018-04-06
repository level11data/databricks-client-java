package com.level11data.databricks.job;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.jobs.RunNowRequestDTO;
import com.level11data.databricks.client.entities.jobs.RunNowResponseDTO;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.job.run.InteractivePythonJobRun;
import com.level11data.databricks.job.run.JobRunException;
import java.util.List;

public class InteractivePythonJob extends InteractiveJob {
    private JobsClient _client;

    public final PythonScript PythonScript;
    public final String[] Parameters;

    public InteractivePythonJob(JobsClient client,
                                InteractiveCluster cluster,
                                PythonScript pythonScript,
                                JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        super(client, cluster, null, jobSettingsDTO);
        _client = client;
        PythonScript = pythonScript;

        //Validate DTO for this Job Type
        JobValidation.validateInteractivePythonJob(jobSettingsDTO);

        Parameters = jobSettingsDTO.SparkPythonTask.Parameters;
    }

    public InteractivePythonJobRun run() throws JobRunException {
        return run(null);
    }

    public InteractivePythonJobRun run(List<String> overrideParameters) throws JobRunException {
        try {
            RunNowRequestDTO runRequestDTO = new RunNowRequestDTO();
            runRequestDTO.JobId = this.Id;

            if(overrideParameters != null) {
                runRequestDTO.PythonParams = overrideParameters.toArray(new String[overrideParameters.size()]);
            }
            RunNowResponseDTO response = _client.runJobNow(runRequestDTO);
            RunDTO jobRun = _client.getRun(response.RunId);
            return new InteractivePythonJobRun(_client, PythonScript, jobRun);
        } catch(HttpException e) {
            throw new JobRunException(e);
        }
    }


}
