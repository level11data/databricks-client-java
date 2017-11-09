package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.client.entities.clusters.AwsAttributesDTO;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.jobs.RunsSubmitRequestDTO;
import com.level11data.databricks.client.entities.jobs.SparkSubmitTaskDTO;

import java.io.InputStream;

/**
 * Example of how to issue a Spark Submit Job
 *
 */
public class SparkSubmitRunNowApp {
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

            RunsSubmitRequestDTO sparkSubmitRequest = new RunsSubmitRequestDTO();
            sparkSubmitRequest.NewCluster = clusterDeffinition;
            sparkSubmitRequest.RunName = "My Streaming App";

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

            sparkSubmitRequest.SparkSubmitTask = sparkSubmitTask;

            //System.out.println("About to make the following request...");
            //System.out.println(sparkSubmitRequest);

            long jobRunId = jClient.submitRun(sparkSubmitRequest).RunId;
            System.out.println("Job Run Id = "+jobRunId);
        } catch (Exception e) {
            System.out.println("Error Exception Thrown: "+ e);
            e.printStackTrace();
        }
    }

}
