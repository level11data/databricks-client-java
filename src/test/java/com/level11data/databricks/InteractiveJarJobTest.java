package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractiveJarJob;
import com.level11data.databricks.job.InteractiveJarJobRun;
import com.level11data.databricks.job.RunState;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class InteractiveJarJobTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public InteractiveJarJobTest() throws Exception {
        loadConfigFromResource();
    }

    private void loadConfigFromResource() throws Exception {
        if(resourceStream == null) {
            throw new IllegalArgumentException("Resource Not Found: " + CLIENT_CONFIG_RESOURCE_NAME);
        }
        _databricksConfig = new DatabricksClientConfiguration(resourceStream);

        _databricks = new DatabricksSession(_databricksConfig);
    }

    @Test
    public void testSimpleInteractiveJarJob() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();
        String dbfsPath = "dbfs:/jason/tmp/test/" + now + "/" + SIMPLE_JAR_RESOURCE_NAME;
        File jarFile = new File(localPath);

        System.out.println(jarFile.toString());

        //Create Interactive Cluster
        String clusterName = "testInteractiveJarJob " + now;
        int numberOfExecutors = 1;

        InteractiveCluster cluster = _databricks.createCluster(clusterName, numberOfExecutors)
                .withAutoTerminationMinutes(20)
                .withSparkVersion("4.0.x-scala2.11") //must be a Spark 1.6.x cluster
                .withNodeType("i3.xlarge")
                .create();

        List<String> baseParams = new ArrayList<String>();
        baseParams.add("hello");
        baseParams.add("world");


        //Create Job
        InteractiveJarJob job = cluster.createJob(new URI(dbfsPath),
                jarFile,
                "com.level11data.example.scala.simpleapp.SimpleApp",
                baseParams)
                .withName(clusterName)
                .create();

        //Run Job
        InteractiveJarJobRun run = job.run();

        while(!run.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(10000); //wait 10 seconds
        }

        System.out.println(run.getRunState());

        //cleanup
        job.delete();
        cluster.terminate();
        _databricks.deleteDbfsObject(dbfsPath, true);
    }
}
