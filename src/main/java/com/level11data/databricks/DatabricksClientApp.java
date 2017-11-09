package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.AutomatedNotebookJobRun;

import java.io.InputStream;
import java.util.HashMap;


public class DatabricksClientApp {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "databricks-client.properties";

    public static void main(String[] args) {
        System.out.println("DatabricksClient- Begin Reading Resource");

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
        if(resourceStream == null) {
            throw new IllegalArgumentException("Resource Not Found: " + CLIENT_CONFIG_RESOURCE_NAME);
        }

        System.out.println("Resource Loaded...");
        try {
            DatabricksClientConfiguration databricksConfig = new DatabricksClientConfiguration(resourceStream);
            DatabricksSession databricks = new DatabricksSession(databricksConfig);

            System.out.println("Use the Client Layer");
            JobsClient jClient = new JobsClient(databricks);
            JobDTO jobDTO = jClient.getJob(5047);
            System.out.println(jobDTO.toString());
            System.out.println("Use the Higher Level API");

            AutomatedNotebookJob job = (AutomatedNotebookJob)databricks.getJob(5047);
            System.out.println("Job Base Parameter Size: " + job.BaseParameters.size());

            System.out.println(job.BaseParameters.toString());

            HashMap<String,String> override = new HashMap<>();
            override.put("parameter1","parameter 1 Override");
            override.put("parameter2", "parameter 2 Override");

            System.out.println("Attempting to Run Job with Parameter Overrides....");
            AutomatedNotebookJobRun run = job.run(override);

            System.out.println("Attempting to Access Run Parameter Overrides....");
            System.out.println("Job Run Parameter Overrides: " + run.OverridingParameters.toString());



        } catch (Exception e) {
            System.out.println("Error Exception Thrown: "+ e);
            e.printStackTrace();
        }
    }
}
