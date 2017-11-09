package com.level11data.databricks.job;

import com.level11data.databricks.client.entities.jobs.JobDTO;

public class JobValidation {


    public static void validateInteractiveNotebookJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobDTO.isInteractive()) {
            throw new JobConfigException("Job is not configured as an Interactive Job");
        } else if(!jobDTO.isNotebookJob()) {
            throw new JobConfigException("Job is not configured as a Notebook Job");
        }
    }

    public static void validateAutomatedNotebookJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated Notebook Job or not
        if(!jobDTO.isAutomated()) {
            throw new JobConfigException("Job is not configured as an Automated Job");
        } else if(!jobDTO.isNotebookJob()) {
            throw new JobConfigException("Job is not configured as a Notebook Job");
        }
    }
}
