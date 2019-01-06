package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterLibrary;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.run.InteractiveNotebookJobRun;
import com.level11data.databricks.job.run.RunResultState;
import com.level11data.databricks.library.JarLibrary;
import com.level11data.databricks.library.LibraryInstallStatus;
import com.level11data.databricks.workspace.Notebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

public class LibraryTest {
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-library_2.11-1.0.jar";
    public static final String DBR_VERSION = "4.3.x-scala2.11";

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    DatabricksSession _databricks = new DatabricksSession(_databricksConfig);

    public LibraryTest() throws Exception {

    }

    @Test
    public void testJarLibraryInteractiveCluster() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "dbfs:/tmp/test/"+uniqueName+"/"+SIMPLE_JAR_RESOURCE_NAME;
        File jarFile = new File(localPath);

        //Create Library and Upload to DBFS
        JarLibrary library = _databricks.getJarLibrary(new URI(dbfsPath));
        library.upload(jarFile);

        //Set cluster name to ClassName.MethodName-TIMESTAMP
        String clusterName = uniqueName;

        //Create Interactive Cluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion(DBR_VERSION)
                .withNodeType("i3.xlarge")
                .withLibrary(library)  //THIS IS WHAT I'm TESTING
                .create();

        while(!cluster.getState().equals(ClusterState.RUNNING)) {
            //System.out.println("Cluster State        : " + cluster.getState().toString());
            //System.out.println("Cluster State Message: " + cluster.getStateMessage());
            Thread.sleep(5000); //wait 5 seconds
        }

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

        //Run an Interactive Notebook Job (without including library) to see if
        // the notebook is able to import the library (which is already attached)
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + "jason@databricks.com" + "/test-notebook-jar-library";
        Notebook notebook = _databricks.getNotebook(notebookPath);

        InteractiveNotebookJob job = cluster.createJob(notebook).withName(clusterName).create();

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Run Was NOT Successful", RunResultState.SUCCESS ,jobRun.getRunState().ResultState);

        Assert.assertEquals("Job Output Does Not Match", "$$ Money Time $$", jobRun.getOutput());

        //Uninstall library
        clusterLibraries.get(0).uninstall();

        //test to make sure that the library is set to be uninstalled upon restart
        Assert.assertEquals("Library Install Status is NOT UNINSTALL_ON_RESTART after uninstall",
                LibraryInstallStatus.UNINSTALL_ON_RESTART, clusterLibraries.get(0).getLibraryStatus().InstallStatus);

        //restart cluster
        cluster.restart();

        while(!cluster.getState().equals(ClusterState.RUNNING)) {
            //System.out.println("Cluster State        : " + cluster.getState().toString());
            //System.out.println("Cluster State Message: " + cluster.getStateMessage());
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
        job.delete();
        _databricks.deleteDbfsObject(dbfsPath, false);
        cluster.terminate();
    }
}
