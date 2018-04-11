package com.level11data.databricks.job;

import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;

public class JobValidation {


    public static void validateInteractiveNotebookJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook AbstractJob or not
        if(!jobDTO.isInteractive()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Interactive AbstractJob");
        } else if(!jobDTO.isNotebookJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Notebook AbstractJob");
        }
    }

    public static void validateInteractiveNotebookJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive Notebook AbstractJob or not
        if(!jobSettingsDTO.isInteractive()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Interactive AbstractJob");
        } else if(!jobSettingsDTO.isNotebookJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Notebook AbstractJob");
        }
    }

    public static void validateInteractiveJarJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive JAR AbstractJob or not
        if(!jobDTO.isInteractive()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Interactive AbstractJob");
        } else if(!jobDTO.isJarJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Jar AbstractJob");
        }
    }

    public static void validateInteractiveJarJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive JAR AbstractJob or not
        if(!jobSettingsDTO.isInteractive()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Interactive AbstractJob");
        } else if(!jobSettingsDTO.isJarJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Jar AbstractJob");
        }
    }

    public static void validateInteractivePythonJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Interactive JAR AbstractJob or not
        if(!jobSettingsDTO.isInteractive()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Interactive AbstractJob");
        } else if(!jobSettingsDTO.isPythonJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Python AbstractJob");
        }
    }

    public static void validateAutomatedNotebookJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated Notebook AbstractJob or not
        if(!jobDTO.isAutomated()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Automated AbstractJob");
        } else if(!jobDTO.isNotebookJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Notebook AbstractJob");
        }
    }

    public static void validateAutomatedNotebookJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated Notebook AbstractJob or not
        if(!jobSettingsDTO.isAutomated()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Automated AbstractJob");
        } else if(!jobSettingsDTO.isNotebookJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Notebook AbstractJob");
        }
    }

    public static void validateAutomatedJarJob(JobDTO jobDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated JAR AbstractJob or not
        if(!jobDTO.isAutomated()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Automated AbstractJob");
        } else if(!jobDTO.isJarJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Jar AbstractJob");
        }
    }

    public static void validateAutomatedJarJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated JAR AbstractJob or not
        if(!jobSettingsDTO.isAutomated()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Automated AbstractJob");
        } else if(!jobSettingsDTO.isJarJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Jar AbstractJob");
        }
    }

    public static void validateAutomatedPythonJob(JobSettingsDTO jobSettingsDTO) throws JobConfigException {
        //Validate if JobDTO is for an Automated Python AbstractJob or not
        if(!jobSettingsDTO.isAutomated()) {
            throw new JobConfigException("AbstractJob is NOT configured as an Automated AbstractJob");
        } else if(!jobSettingsDTO.isPythonJob()) {
            throw new JobConfigException("AbstractJob is NOT configured as a Python AbstractJob");
        }
    }


}
