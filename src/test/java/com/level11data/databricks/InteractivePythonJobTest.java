package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractivePythonJob;
import com.level11data.databricks.job.PythonScript;
import com.level11data.databricks.job.run.InteractivePythonJobRun;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class InteractivePythonJobTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_PYTHON_RESOURCE_NAME = "simpleapp.py";
    public static final String DBR_VERSION = "4.3.x-scala2.11";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public InteractivePythonJobTest() throws Exception {
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
    public void testSimpleInteractivePythonJob() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_PYTHON_RESOURCE_NAME).getFile();

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "dbfs:/tmp/test/" + uniqueName + "/" + SIMPLE_PYTHON_RESOURCE_NAME;
        File pythonFile = new File(localPath);

        //Create Python Script
        PythonScript pythonScript = _databricks.getPythonScript(new URI(dbfsPath));

        //Set cluster name to ClassName.MethodName TIMESTAMP
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //Create Interactive Cluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion(DBR_VERSION)
                .withNodeType("i3.xlarge")
                .create();

        List<String> baseParams = new ArrayList<String>();
        baseParams.add("hello");
        baseParams.add("world");

        //Create Job
        InteractivePythonJob job = cluster.createJob(pythonScript, pythonFile, baseParams)
                .withName(clusterName)
                .create();

        //Run Job
        InteractivePythonJobRun run = job.run();

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
