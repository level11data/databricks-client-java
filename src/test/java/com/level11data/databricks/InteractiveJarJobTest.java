package com.level11data.databricks;

import com.level11data.databricks.session.WorkspaceSession;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractiveJarJob;
import com.level11data.databricks.job.run.InteractiveJarJobRun;
import com.level11data.databricks.library.JarLibrary;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class InteractiveJarJobTest {
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    public final String DBR_VERSION = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.sparkVersion");

    public final String NODE_TYPE = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.nodeType");

    WorkspaceSession _databricks = new WorkspaceSession(_databricksConfig);

    public InteractiveJarJobTest() throws Exception {

    }

    @Test
    public void testSimpleInteractiveJarJob() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "dbfs:/tmp/test/" + uniqueName + "/" + SIMPLE_JAR_RESOURCE_NAME;
        File jarFile = new File(localPath);

        //System.out.println(jarFile.toString());

        //Set cluster name to ClassName.MethodName TIMESTAMP
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //Create Interactive Cluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion(DBR_VERSION) //must be a Spark 1.6.x cluster
                .withNodeType(NODE_TYPE)
                .create();

        List<String> baseParams = new ArrayList<String>();
        baseParams.add("hello");
        baseParams.add("world");

        JarLibrary jarLibrary = _databricks.getJarLibrary(new URI(dbfsPath));

        //Create Job
        InteractiveJarJob job = cluster.createJob(jarLibrary,
                "com.level11data.example.scala.simpleapp.SimpleApp",
                jarFile,
                baseParams)
                .withName(clusterName)
                .create();

        //Run Job
        InteractiveJarJobRun run = job.run();

        while(!run.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(10000); //wait 10 seconds
        }

        //System.out.println(run.getRunState());

        //cleanup
        job.delete();
        cluster.terminate();
        _databricks.deleteDbfsObject(dbfsPath, true);
    }
}
