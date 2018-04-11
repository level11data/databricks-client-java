package com.level11data.databricks.job.util;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.JobRunOutputDTO;
import com.level11data.databricks.job.run.JobRunException;

public class JobRunHelper {

    public static String getJobRunOutput(JobsClient client, long runId) throws HttpException, JobRunException {
        //otherwise, get the notebook output, if any
        JobRunOutputDTO jobRunOutput = client.getRunOutput(runId);

        if(jobRunOutput.NotebookOutput != null) {
            if(jobRunOutput.NotebookOutput.Result != null) {
                return jobRunOutput.NotebookOutput.Result;
            }
        } else if(jobRunOutput.Error != null) {
            throw new JobRunException(jobRunOutput.Error);
        }

        //No AbstractJob Run Output was found; Nor was an error
        return null;
    }
}
