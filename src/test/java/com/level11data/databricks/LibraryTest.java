package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.ClusterState;
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
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-library_2.11-1.0.jar";

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
    public void testJarLibraryInteractiveCluster() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();
        String dbfsPath = "dbfs:/jason/tmp/test/"+now+"/spark-simpleapp-sbt_2.10-1.0.jar";
        File jarFile = new File(localPath);

        //Create Library and Upload to DBFS
        JarLibrary library = _databricks.getJarLibrary(new URI(dbfsPath));
        library.upload(jarFile);

        //Create Interactive Cluster
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;
        int numberOfExecutors = 1;

        InteractiveCluster cluster = _databricks.createCluster(clusterName, numberOfExecutors)
                .withAutoTerminationMinutes(20)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .withLibrary(library)  //THIS IS WHAT I'm TESTING
                .create();

        System.out.println("Cluster State        : " + cluster.getState().toString());
        System.out.println("Cluster State Message: " + cluster.getStateMessage());

        while(!cluster.getState().equals(ClusterState.RUNNING)) {
            System.out.println("Cluster State        : " + cluster.getState().toString());
            System.out.println("Cluster State Message: " + cluster.getStateMessage());
            Thread.sleep(5000); //wait 5 seconds
        }

        System.out.println("Cluster State        : " + cluster.getState().toString());
        System.out.println("Cluster State Message: " + cluster.getStateMessage());

        //test to make sure that library was attached to cluster
        List<ClusterLibrary> clusterLibraries = cluster.getLibraries();
        Assert.assertEquals("Number of installed libraries is NOT 1",
                1, clusterLibraries.size());

        //test to make sure the library is the right type (JarLibrary)
        Assert.assertEquals("Installed library on cluster is NOT a JarLibrary",
                JarLibrary.class.getTypeName(), clusterLibraries.get(0).Library.getClass().getTypeName());

        //test to make sure that the library on the cluster is the same object reference
        Assert.assertEquals("Library object reference does NOT match",
                library, clusterLibraries.get(0).Library);

        while(!clusterLibraries.get(0).getLibraryStatus().InstallStatus.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        //test to make sure cluster library status is INSTALLED
        Assert.assertEquals("Library Install Status is NOT INSTALLED",
                LibraryInstallStatus.INSTALLED, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        //Uninstall library
        clusterLibraries.get(0).uninstall();

        //test to make sure that the library is set to be uninstalled upon restart
        Assert.assertEquals("Library Install Status is NOT UNINSTALL_ON_RESTART after uninstall",
                LibraryInstallStatus.UNINSTALL_ON_RESTART, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        //restart cluster
        cluster.restart();

        while(!cluster.getState().equals(ClusterState.RUNNING)) {
            System.out.println("Cluster State        : " + cluster.getState().toString());
            System.out.println("Cluster State Message: " + cluster.getStateMessage());
            Thread.sleep(5000); //wait 5 seconds
        }

        //test that the cluster library list removes the libraries after an uninstall and restart
        Assert.assertEquals("Number of installed libraries is NOT 0 after uninstall and restart",
                0, cluster.getLibraries().size());

        //test that library will attach successfully to running cluster
        cluster.installLibrary(library);

        Assert.assertEquals("Number of installed libraries after re-install is NOT 1",
                1, cluster.getLibraries().size());

        //cleanup the test
        _databricks.deleteDbfsObject(dbfsPath, false);
        cluster.terminate();
    }


    @Test
    public void testInteractiveNotebookJobWithDbfsLibrary() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();
        String dbfsPath = "dbfs:/jason/tmp/test/"+now+"/"+SIMPLE_JAR_RESOURCE_NAME;
        File jarFile = new File(localPath);

        //Create Library and Upload to DBFS
        JarLibrary library = _databricks.getJarLibrary(new URI(dbfsPath));
        library.upload(jarFile);

        //Create Interactive Cluster
        String clusterName = this.getClass().getName() + " " + now;
        int numberOfExecutors = 1;

        InteractiveCluster cluster = _databricks.createCluster(clusterName, numberOfExecutors)
                .withAutoTerminationMinutes(20)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .create();


        //System.out.println("Cluster State        : " + cluster.getState().toString());
        //System.out.println("Cluster State Message: " + cluster.getStateMessage());

        while(!cluster.getState().equals(ClusterState.RUNNING)) {
            //System.out.println("Cluster State        : " + cluster.getState().toString());
            //System.out.println("Cluster State Final  : " + cluster.getState().isFinal());
            //System.out.println("Cluster State Message: " + cluster.getStateMessage());
            Thread.sleep(5000); //wait 5 seconds
        }

        //System.out.println("Cluster State        : " + cluster.getState().toString());
        //System.out.println("Cluster State Message: " + cluster.getStateMessage());

        //Install Library - maybe have to wait until after cluster is started?
        cluster.installLibrary(library);

        List<ClusterLibrary> clusterLibraries = cluster.getLibraries();
        Assert.assertEquals("Number of installed libraries is NOT 1",
                1, clusterLibraries.size());

        Assert.assertEquals("Installed library on cluster is NOT a JarLibrary",
                JarLibrary.class.getTypeName(), clusterLibraries.get(0).Library.getClass().getTypeName());

        Assert.assertEquals("Library object reference does NOT match",
                library, clusterLibraries.get(0).Library);

        while(!clusterLibraries.get(0).getLibraryStatus().InstallStatus.isFinal()){
            System.out.println("InstallStatus="+clusterLibraries.get(0).getLibraryStatus().InstallStatus);
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Library Install Status is NOT INSTALLED",
                LibraryInstallStatus.INSTALLED, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        if(clusterLibraries.get(0).getLibraryStatus().Messages != null) {
            int index = 0;
            while(index < clusterLibraries.get(0).getLibraryStatus().Messages.length) {
                System.out.println("Messages="+clusterLibraries.get(0).getLibraryStatus().Messages[index].toString());
                index ++;
            }
        }

        //create job - Job keeps failing due to Scala version conflicts, but I can get it to run manually through UI
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + "jason@databricks.com" + "/test-notebook-jar-library";
        Notebook notebook = new Notebook(notebookPath);

        InteractiveNotebookJob job = cluster.createJob(notebook)
                .withName("testRunningInteractiveClusterDbfsLibrary "+now)
                .create();

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Output Does Not Match", "$$ Money Time $$",
                jobRun.getOutput());

        //cleanup
        _databricks.deleteDbfsObject(dbfsPath, false);
        job.delete();
        cluster.terminate();
    }
}