package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.InteractiveNotebookJobRun;
import com.level11data.databricks.library.JarLibrary;
import com.level11data.databricks.library.LibraryInstallStatus;
import com.level11data.databricks.workspace.Notebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

public class LibraryTest {
    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_JAR_RESOURCE_NAME = "spark-simpleapp-sbt_2.10-1.0.jar";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public LibraryTest() throws Exception {
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
    public void testRunningInteractiveClusterDbfsLibrary() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile(); //less than 1MB
        String dbfsPath = "dbfs:/jason/tmp/test/"+now+"/spark-simpleapp-sbt_2.10-1.0.jar";
        File jarFile = new File(localPath);

        //Create Library and Upload to DBFS
        JarLibrary library = _databricks.getJarLibrary(new URI(dbfsPath));
        library.upload(jarFile);

        //Create Interactive Cluster
        String clusterName = "testRunningInteractiveClusterDbfsLibrary " + now;
        int numberOfExecutors = 1;

        InteractiveCluster cluster = _databricks.createCluster(clusterName, numberOfExecutors)
                .withAutoTerminationMinutes(20)
                .withSparkVersion("1.6.3-db2-hadoop2-scala2.10")  //library requires spark 1.6 scala 2.10
                .withNodeType("i3.xlarge")
                .create();

        //Install Library
        cluster.installLibrary(library);

        List<ClusterLibrary> clusterLibraries = cluster.getLibraries();
        Assert.assertEquals("Number of installed libraries is NOT 1",
                1, clusterLibraries.size());

        Assert.assertEquals("Installed library on cluster is NOT a JarLibrary",
                JarLibrary.class.getTypeName(), clusterLibraries.get(0).Library.getClass().getTypeName());

        Assert.assertEquals("Library object reference does NOT match",
                library, clusterLibraries.get(0).Library);

        Assert.assertEquals("Library Install Status is NOT PENDING",
                LibraryInstallStatus.PENDING, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        while(clusterLibraries.get(0).getLibraryStatus().InstallStatus == LibraryInstallStatus.PENDING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Library Install Status is NOT INSTALLING",
                LibraryInstallStatus.INSTALLING, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        while(clusterLibraries.get(0).getLibraryStatus().InstallStatus == LibraryInstallStatus.INSTALLING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        //System.out.println("InstallStatus="+clusterLibraries.get(0).getLibraryStatus().InstallStatus);
        //System.out.println("Messages="+clusterLibraries.get(0).getLibraryStatus().Messages.toString());

        //int index = 0;
        //while(index < clusterLibraries.get(0).getLibraryStatus().Messages.length) {
        //    System.out.println("Messages="+clusterLibraries.get(0).getLibraryStatus().Messages[index].toString());
        //    index ++;
        //}

        Assert.assertEquals("Library Install Status is NOT INSTALLED",
                LibraryInstallStatus.INSTALLED, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        //create job
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + "jason@databricks.com" + "/test-notebook-jar-library";
        Notebook notebook = new Notebook(notebookPath);

        HashMap<String,String> parameters = new HashMap<String,String>();
        parameters.put("parameter1", "Hello");
        parameters.put("parameter2", "World");

        InteractiveNotebookJob job = cluster.createJob(notebook, parameters)
                .withName("testRunningInteractiveClusterDbfsLibrary "+now)
                .create();

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        //TODO - Create a 2.11 Spark 2.x JAR to test
        Assert.assertEquals("Job Output Does Not Match", "4",
                jobRun.getOutput());

        //cleanup
        _databricks.deleteDbfsObject(dbfsPath, false);
        job.delete();
        cluster.terminate();
    }

}
