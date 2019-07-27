package com.level11data.databricks;

import com.level11data.databricks.session.WorkspaceSession;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.AutomatedSparkSubmitJob;
import com.level11data.databricks.job.run.AutomatedSparkSubmitJobRun;
import com.level11data.databricks.job.run.RunResultState;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AutomatedSparkSubmitJobTest {

    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    WorkspaceSession _databricks = new WorkspaceSession(_databricksConfig);

    public final String DBR_VERSION = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.sparkVersion");

    public final String NODE_TYPE = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.nodeType");


    public AutomatedSparkSubmitJobTest() throws Exception {

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
                .withSparkVersion(DBR_VERSION)
                .withNodeType(NODE_TYPE)
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
