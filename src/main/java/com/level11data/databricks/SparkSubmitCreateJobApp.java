package com.level11data.databricks;


import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.client.entities.clusters.AwsAttributesDTO;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.jobs.JobSettingsDTO;
import com.level11data.databricks.client.entities.jobs.SparkSubmitTaskDTO;

import java.io.InputStream;

public class SparkSubmitCreateJobApp {
    public static final String CLIENT_CONFIG_RESOURCE_NAME = "databricks-client.properties";

    public static void main(String[] args) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
        if(resourceStream == null) {
            throw new IllegalArgumentException("Resource Not Found: " + CLIENT_CONFIG_RESOURCE_NAME);
        }
        System.out.println("Databricks Config Loaded...");
        try {
            DatabricksClientConfiguration databricksConfig = new DatabricksClientConfiguration(resourceStream);
            DatabricksSession databricks = new DatabricksSession(databricksConfig);

            System.out.println("Username: " + databricksConfig.getClientUsername());
            System.out.println("URL: " + databricksConfig.getClientUrl());

            JobsClient jClient = new JobsClient(databricks);

            //Spark Submit Run
            ClusterInfoDTO clusterDeffinition = new ClusterInfoDTO();
            clusterDeffinition.SparkVersionKey = "3.2.x-scala2.11";
            clusterDeffinition.NodeTypeId = "i3.xlarge";
            clusterDeffinition.NumWorkers = 1;
            AwsAttributesDTO awsAttrib = new AwsAttributesDTO();
            awsAttrib.Availability = "ON_DEMAND";
            clusterDeffinition.AwsAttributes = awsAttrib;

            JobSettingsDTO jobSettings = new JobSettingsDTO();
            jobSettings.Name = "My Job Name";

            jobSettings.NewCluster = clusterDeffinition;

            String[] sparkSubmitParams = {"--driver-java-options",
                    "-Dlog4j.configurationFile=/dbfs/mnt/log4j/log4j2.xml",
                    "-Dlog4j.configuration=/dbfs/mnt/log4j/log4j.properties",
                    "--conf",
                    "spark.executor.extraJavaOptions=-Dlog4j.configuration=/dbfs/mnt/log4j/log4j2.xml",
                    "--class",
                    "com.level11data.SparkSubmitApp.main",
                    "/dbfs/mnt/level11data-app-0.1-SNAPSHOT.jar",
                    "--config",
                    "/dbfs/mnt/config/config.yaml"
            };

            SparkSubmitTaskDTO sparkSubmitTask = new SparkSubmitTaskDTO();
            sparkSubmitTask.Parameters = sparkSubmitParams;
            jobSettings.SparkSubmitTask = sparkSubmitTask;

            //System.out.println("About to make the following request...");
            //System.out.println(jobSettings);

            long jobId = jClient.createJob(jobSettings);
            System.out.println("JobId="+jobId);
        } catch (Exception e) {
            System.out.println("Error Exception Thrown: "+ e);
            e.printStackTrace();
        }
    }

}
