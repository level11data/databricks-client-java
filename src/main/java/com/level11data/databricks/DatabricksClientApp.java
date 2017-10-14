package com.level11data.databricks;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.config.DatabricksClientConfiguration;

import java.io.InputStream;


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

            System.out.println("Username: " + databricksConfig.getClientUsername());
            System.out.println("URL: " + databricksConfig.getClientUrl());

            ClustersClient cClient = new ClustersClient(databricks);
            //System.out.println(cClient.getSparkVersions().toString());
            System.out.println(cClient.getNodeTypes().toString());

            //JobsClient jClient = new JobsClient(databricks);
            //System.out.println(jClient.listJobs().toString());

            //JobDTO job = jClient.getJob(4149);
            //System.out.println(job.toString());



        } catch (Exception e) {
            System.out.println("Error Exception Thrown: "+ e);
            e.printStackTrace();
        }
    }
}
