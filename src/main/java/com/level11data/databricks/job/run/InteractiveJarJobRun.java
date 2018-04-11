package com.level11data.databricks.job.run;

import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import java.util.*;

public class InteractiveJarJobRun extends AbstractInteractiveJobRun implements JobRun {
    public final List<String> BaseParameters;
    public final List<String> OverridingParameters;

    public InteractiveJarJobRun(JobsClient client, RunDTO runDTO) throws JobRunException {
        super(client, runDTO);

        if(!runDTO.isJarJob()) {
            throw new JobRunException("AbstractJob Run is not configured as a Jar AbstractJob");
        }

        //Set Base Parameters of Run
        BaseParameters = this.getBaseParametersAsList();

        //Set Overriding Parameters of Run
        OverridingParameters = this.getOverridingParametersAsList();
    }

}
