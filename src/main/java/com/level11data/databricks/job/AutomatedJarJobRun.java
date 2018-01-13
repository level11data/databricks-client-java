package com.level11data.databricks.job;


import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.library.LibraryConfigException;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AutomatedJarJobRun extends AutomatedJobRun {
    private JobsClient _client;

    public final List<String> BaseParameters;
    public final List<String> OverridingParameters;

    public AutomatedJarJobRun(JobsClient client, RunDTO runDTO) throws JobRunException, URISyntaxException, LibraryConfigException {
        super(client, runDTO);
        _client = client;

        if(!runDTO.isJarJob()) {
            throw new JobRunException("Job Run is not configured as a JAR Job");
        }

        if(runDTO.Task.SparkJarTask.Parameters != null) {
            ArrayList<String> params = new ArrayList<>();

            for (String param : runDTO.Task.SparkJarTask.Parameters) {
                params.add(param);
            }
            BaseParameters = Collections.unmodifiableList(params);
        } else {
            BaseParameters = Collections.unmodifiableList(new ArrayList<String>());
        }

        if(runDTO.OverridingParameters != null) {
            if(runDTO.OverridingParameters.JarParams != null) {
                ArrayList<String> params = new ArrayList<>();

                for (String param : runDTO.OverridingParameters.JarParams) {
                    params.add(param);
                }
                OverridingParameters = Collections.unmodifiableList(params);
            } else {
                OverridingParameters = Collections.unmodifiableList(new ArrayList<String>());
            }
        } else {
            OverridingParameters = Collections.unmodifiableList(new ArrayList<String>());
        }

    }

}
