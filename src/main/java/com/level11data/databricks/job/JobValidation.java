package com.level11data.databricks.job;

import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;

public class JobValidation {


    public static void validateInteractiveNotebookJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobDTO.isInteractive()) {
            throw new JobConfigException("Job is NOT configured as an Interactive Job");
        } else if(!jobDTO.isNotebookJob()) {
            throw new JobConfigException("Job is NOT configured as a Notebook Job");
        }
    }

    public static void validateInteractiveNotebookJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobSettingsDTO.isInteractive()) {
            throw new JobConfigException("Job is NOT configured as an Interactive Job");
        } else if(!jobSettingsDTO.isNotebookJob()) {
            throw new JobConfigException("Job is NOT configured as a Notebook Job");
        }
    }

    public static void validateInteractiveJarJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobDTO.isInteractive()) {
            throw new JobConfigException("Job is NOT configured as an Interactive Job");
        } else if(!jobDTO.isJarJob()) {
            throw new JobConfigException("Job is NOT configured as a Jar Job");
        }
    }

    public static void validateInteractiveJarJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobSettingsDTO.isInteractive()) {
            throw new JobConfigException("Job is NOT configured as an Interactive Job");
        } else if(!jobSettingsDTO.isJarJob()) {
            throw new JobConfigException("Job is NOT configured as a Jar Job");
        }
    }

    public static void validateAutomatedNotebookJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated Notebook Job or not
        if(!jobDTO.isAutomated()) {
            throw new JobConfigException("Job is NOT configured as an Automated Job");
        } else if(!jobDTO.isNotebookJob()) {
            throw new JobConfigException("Job is NOT configured as a Notebook Job");
        }
    }

    public static void validateAutomatedNotebookJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated Notebook Job or not
        if(!jobSettingsDTO.isAutomated()) {
            throw new JobConfigException("Job is NOT configured as an Automated Job");
        } else if(!jobSettingsDTO.isNotebookJob()) {
            throw new JobConfigException("Job is NOT configured as a Notebook Job");
        }
    }

    public static void validateAutomatedJarJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobDTO.isAutomated()) {
            throw new JobConfigException("Job is NOT configured as an Automated Job");
        } else if(!jobDTO.isJarJob()) {
            throw new JobConfigException("Job is NOT configured as a Jar Job");
        }
    }

    public static void validateAutomatedJarJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook Job or not
        if(!jobSettingsDTO.isAutomated()) {
            throw new JobConfigException("Job is NOT configured as an Automated Job");
        } else if(!jobSettingsDTO.isJarJob()) {
            throw new JobConfigException("Job is NOT configured as a Jar Job");
        }
    }

}
