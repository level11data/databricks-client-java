package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.dbfs.DbfsHelper;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.AutomatedSparkSubmitJob;
import com.level11data.databricks.job.run.AutomatedJarJobRun;
import com.level11data.databricks.job.run.AutomatedSparkSubmitJobRun;
import com.level11data.databricks.job.run.RunResultState;
import com.level11data.databricks.library.JarLibrary;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class AutomatedSparkSubmitJobTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public AutomatedSparkSubmitJobTest() throws Exception {
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

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "dbfs:/tmp/test/" + uniqueName + "/" + SIMPLE_JAR_RESOURCE_NAME;
        File jarFile = new File(localPath);

        //Put file onto DBFS
        _databricks.putDbfsFile(jarFile, dbfsPath);

        //create cluster spec
        ClusterSpec clusterSpec = _databricks.createClusterSpec(1)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .createClusterSpec();

        //create job
        String jobName = uniqueName;
        List<String> params = new ArrayList<String>();
        params.add("--class");
        params.add("com.level11data.example.scala.simpleapp.SimpleApp");
        params.add(dbfsPath);
        params.add("hello");
        params.add("world");

        AutomatedSparkSubmitJob job = _databricks.createJob(params)
                .withName(jobName)
                .withClusterSpec(clusterSpec)
                .create();

        //run job
        AutomatedSparkSubmitJobRun run = job.run();

        while(!run.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(10000); //sleep 10 seconds
        }

        Assert.assertEquals("Job Run was NOT Successful", RunResultState.SUCCESS,
                run.getRunState().ResultState);

        //cleanup
        job.delete();
        _databricks.deleteDbfsObject(dbfsPath, true);
    }


}
