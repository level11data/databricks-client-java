package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.AutomatedPythonJob;
import com.level11data.databricks.job.PythonScript;
import com.level11data.databricks.job.run.AutomatedPythonJobRun;
import com.level11data.databricks.job.run.RunResultState;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class AutomatedPythonJobTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_PYTHON_RESOURCE_NAME = "simpleapp.py";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public AutomatedPythonJobTest() throws Exception {
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
    public void testSimpleAutomatedPythonJob() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_PYTHON_RESOURCE_NAME).getFile();

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "dbfs:/tmp/test/" + uniqueName + "/" + SIMPLE_PYTHON_RESOURCE_NAME;
        File pythonFile = new File(localPath);
        PythonScript pythonScript = _databricks.getPythonScript(new URI(dbfsPath));

        //create job
        String jobName = uniqueName;
        List<String> params = new ArrayList<>();
        params.add("hello");
        params.add("world");

        //create cluster spec
        ClusterSpec clusterSpec = _databricks.createClusterSpec(1)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .createClusterSpec();

        AutomatedPythonJob job = _databricks.createJob(pythonScript, pythonFile, params)
                .withName(jobName)
                .withClusterSpec(clusterSpec)
                .create();

        //run job
        AutomatedPythonJobRun run = job.run();

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
