package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.AutomatedJarJobRun;
import com.level11data.databricks.job.builder.AutomatedJarJobBuilder;
import com.level11data.databricks.library.JarLibrary;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class AutomatedJarJobTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public AutomatedJarJobTest() throws Exception {
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
    public void testSimpleAutomatedJarJob() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();
        String dbfsPath = "dbfs:/jason/tmp/test/" + now + "/" + SIMPLE_JAR_RESOURCE_NAME;
        File jarFile = new File(localPath);
        JarLibrary jarLibrary = _databricks.getJarLibrary(new URI(dbfsPath));


        //create job
        String jobName = "testAutomatedJarJob " + now;
        String mainClass = "com.level11data.example.scala.simpleapp.SimpleApp";
        List<String> params = new ArrayList<String>();
        params.add("hello");
        params.add("world");

        AutomatedJarJob job = _databricks.createJob(jarLibrary, mainClass, jarFile, params)
                .withName(jobName)
                .withClusterSpec(1)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .addToJob(AutomatedJarJobBuilder.class)
                .create();

        //run job
        AutomatedJarJobRun run = job.run();

        while(!run.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(10000); //sleep 10 seconds
        }

        System.out.println(run.getRunState().LifeCycleState.toString());

        //cleanup
        //job.delete();
        //cluster.terminate();
        //_databricks.deleteDbfsObject(dbfsPath, true);
    }


}
