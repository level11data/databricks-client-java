package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryRequestDTO;
import com.level11data.databricks.client.entities.libraries.ClusterLibraryStatusesDTO;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.LibraryFullStatusDTO;
import com.level11data.databricks.job.builder.InteractiveNotebookJobBuilder;
import com.level11data.databricks.library.*;
import com.level11data.databricks.workspace.Notebook;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class InteractiveCluster extends Cluster{
    private ClustersClient _client;
    private LibrariesClient _librariesClient;
    private Boolean _isAutoScaling = false;
    private JobsClient _jobsClient;
    private ArrayList<ClusterLibrary> _libraries = new ArrayList<ClusterLibrary>();

    public final String Name;
    public final Integer NumWorkers;
    public final AutoScale AutoScale;
    public final Integer AutoTerminationMinutes;


    /**
     * Represents a Databricks InteractiveCluster.
     *
     * Is instantiated by
     *   1. InteractiveClusterBuilder.create()
     *   2. DatabricksSession.getCluster() by Id
     *   3. InteractiveCluster.resize()
     *
     * @param client Databricks ClusterClient
     * @param info Databricks ClusterInfoDTO POJO
     * @throws ClusterConfigException
     */
    public InteractiveCluster(ClustersClient client, ClusterInfoDTO info) throws ClusterConfigException, HttpException {
        super(client, info);
        _client = client;

        //Validate that required fields are populated in the ClusterInfoDTO
        validateClusterInfo(info);

        Name = info.ClusterName;
        NumWorkers = info.NumWorkers;

        if(info.AutoScale != null){
            _isAutoScaling = true;
            AutoScale = new AutoScale(info.AutoScale);
        } else {
            AutoScale = null;
        }

        //Set fields that do not change throughout the lifespan of a cluster configuration
        // these fields may not have been set in the DTO if object was instantiated from InteractiveClusterBuilder.create()
        AutoTerminationMinutes = initAutoTerminationMinutes();
    }

    private void validateClusterInfo(ClusterInfoDTO info) throws ClusterConfigException {
        if(info.ClusterName == null) {
            throw new ClusterConfigException("ClusterInfoDTO Must Have Name");
        }

        if(info.NumWorkers == null && info.AutoScale == null)  {
            throw new ClusterConfigException("ClusterInfoDTO Must Have either NumWorkers OR AutoScaleDTO");
        }
    }

    private Integer initAutoTerminationMinutes() throws HttpException  {
        return getClusterInfo().AutoTerminationMinutes;
    }

    public void start() throws HttpException {
        _client.start(Id);
    }

    public void restart() throws HttpException {
        _client.reStart(Id);
    }

    public void terminate() throws HttpException {
        _client.delete(Id);
    }

    public InteractiveCluster resize(Integer numWorkers) throws ClusterConfigException, HttpException {
        if(_isAutoScaling) {
            throw new ClusterConfigException("Must Include New Min and Max Worker Values when Resizing an Autoscaling InteractiveCluster");
        }
        _client.resize(Id, numWorkers);

        ClusterInfoDTO resizedClusterConfig = getClusterInfo();
        resizedClusterConfig.NumWorkers = numWorkers;
        return new InteractiveCluster(_client, resizedClusterConfig);
    }

    public InteractiveCluster resize(Integer minWorkers, Integer maxWorkers) throws ClusterConfigException, HttpException {
        if(!_isAutoScaling) {
            throw new ClusterConfigException("Must Only Include a Single Value When Resizing a Fixed Size InteractiveCluster");
        }
        _client.resize(Id, minWorkers, maxWorkers);

        ClusterInfoDTO resizedClusterConfig = getClusterInfo();
        resizedClusterConfig.AutoScale.MinWorkers = minWorkers;
        resizedClusterConfig.AutoScale.MaxWorkers = maxWorkers;
        return new InteractiveCluster(_client, resizedClusterConfig);
    }

    private JobsClient getOrCreateJobsClient() {
        if(_jobsClient == null) {
            _jobsClient = new JobsClient(_client.Session);
        }
        return _jobsClient;
    }

    public InteractiveNotebookJobBuilder createJob(Notebook notebook) {
      return new InteractiveNotebookJobBuilder(getOrCreateJobsClient(), this, notebook);
    }

    public InteractiveNotebookJobBuilder createJob(Notebook notebook, Map<String,String> baseParameters) {
        return new InteractiveNotebookJobBuilder(getOrCreateJobsClient(), this, notebook, baseParameters);
    }

    private LibrariesClient getLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient = new LibrariesClient(_client.Session);
        }
        return _librariesClient;
    }

    public void installLibrary(JarLibrary library) throws HttpException {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = library.Uri.toString();

        ClusterLibraryRequestDTO libraryRequestDTO = new ClusterLibraryRequestDTO();
        libraryRequestDTO.ClusterId = this.Id;
        libraryRequestDTO.Libraries = new LibraryDTO[1];
        libraryRequestDTO.Libraries[0] = libraryDTO;

        getLibrariesClient().installLibraries(libraryRequestDTO);
        _libraries.add(new ClusterLibrary(this, library));
    }

    public void uninstallLibrary(JarLibrary library) throws HttpException {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = library.Uri.toString();

        ClusterLibraryRequestDTO libraryRequestDTO = new ClusterLibraryRequestDTO();
        libraryRequestDTO.ClusterId = this.Id;
        libraryRequestDTO.Libraries = new LibraryDTO[1];
        libraryRequestDTO.Libraries[0] = libraryDTO;

        getLibrariesClient().uninstallLibraries(libraryRequestDTO);
    }

    public List<ClusterLibrary> getLibraries() throws LibraryConfigException, HttpException, URISyntaxException {
        //add remote libraries to cached list (if NOT already in list)
        ClusterLibraryStatusesDTO remoteLibStatuses = getLibrariesClient().getClusterStatus(this.Id);
        for (LibraryFullStatusDTO libStat : remoteLibStatuses.LibraryStatuses) {
            Library library = createLibraryFromDTO(libStat);
            ClusterLibrary clusterLibrary = getClusterLibraryFromCache(library);
            if(clusterLibrary == null) {
                _libraries.add(new ClusterLibrary(this, library));
            }
        }
        //remove ClusterLibrary from cached list if it is NOT in the remote library status list
        //avoid ConcurrentModificationException by using the list's iterator
        Iterator<ClusterLibrary> iterClusterLibrary = _libraries.iterator();
        while(iterClusterLibrary.hasNext()) {
          ClusterLibrary clusterLib = iterClusterLibrary.next();
          if(getLibraryFromDTO(remoteLibStatuses, clusterLib.Library) == null) {
              iterClusterLibrary.remove();
          }
        }
        return Collections.unmodifiableList(_libraries);
    }

    private ClusterLibrary getClusterLibraryFromCache(Library library) throws HttpException {
        for (ClusterLibrary clusterLibrary : _libraries) {
            Library cachedLibrary = clusterLibrary.Library;
            if(cachedLibrary instanceof JarLibrary && library instanceof JarLibrary) {
                if(((JarLibrary) cachedLibrary).Uri.equals(((JarLibrary) library).Uri)) {
                    return clusterLibrary;
                }
            } else if(cachedLibrary instanceof EggLibrary && library instanceof EggLibrary) {
                if(((EggLibrary) cachedLibrary).Uri.equals(((EggLibrary) library).Uri)) {
                    return clusterLibrary;
                }
            } else if(cachedLibrary instanceof MavenLibrary && library instanceof MavenLibrary) {
                if(((MavenLibrary) cachedLibrary).Coordinates.equals(((MavenLibrary) library).Coordinates)) {
                    return clusterLibrary;
                }
            } else if(cachedLibrary instanceof PyPiLibrary && library instanceof PyPiLibrary) {
                if(((PyPiLibrary) cachedLibrary).PackageName.equals(((PyPiLibrary) library).PackageName)) {
                    return clusterLibrary;
                }
            } else if(cachedLibrary instanceof CranLibrary && library instanceof CranLibrary) {
                if(((CranLibrary) cachedLibrary).PackageName.equals(((CranLibrary) library).PackageName)) {
                    return clusterLibrary;
                }
            }
        }
        //no ClusterLibrary matches the library
        return null;
    }

    private Library getLibraryFromDTO(ClusterLibraryStatusesDTO remoteLibStatuses,
                                      Library library) throws HttpException {
        for (LibraryFullStatusDTO remoteLibStatus : remoteLibStatuses.LibraryStatuses) {
            if(remoteLibStatus.Library.Jar != null && library instanceof JarLibrary) {
                if(((JarLibrary) library).Uri.toString().equals(remoteLibStatus.Library.Jar)) {
                    //System.out.println("DEBUG InteractiveCluster.getLibraryFromDTO library.Uri="+((JarLibrary) library).Uri.toString());
                    //System.out.println("DEBUG InteractiveCluster.getLibraryFromDTO remoteLibStatus.Library.Jar=" + remoteLibStatus.Library.Jar);
                    return library;
                }
            } else if(remoteLibStatus.Library.Egg != null && library instanceof EggLibrary) {
                if(((EggLibrary) library).Uri.toString().equals(remoteLibStatus.Library.Egg)) {
                    return library;
                }
            } else if(remoteLibStatus.Library.Maven != null && library instanceof MavenLibrary) {
                if(((MavenLibrary) library).Coordinates.equals(remoteLibStatus.Library.Maven.Coordinates)) {
                    return library;
                }
            } else if(remoteLibStatus.Library.PyPi != null && library instanceof PyPiLibrary) {
                if(((PyPiLibrary) library).PackageName.equals(remoteLibStatus.Library.PyPi.Package)) {
                    return library;
                }
            } else if(remoteLibStatus.Library.Cran != null && library instanceof CranLibrary) {
                if(((CranLibrary) library).PackageName.equals(remoteLibStatus.Library.Cran.Package)) {
                    return library;
                }
            }
        }
        //Library could not be found in DTO
        return null;
    }

    private Library createLibraryFromDTO(LibraryFullStatusDTO libraryStatus)
            throws LibraryConfigException, URISyntaxException {
        if(libraryStatus.Library.Jar != null) {
            return new JarLibrary(getLibrariesClient(), new URI(libraryStatus.Library.Jar));
        } else if(libraryStatus.Library.Egg != null) {
            return new EggLibrary(getLibrariesClient(), new URI(libraryStatus.Library.Egg));
        } else if (libraryStatus.Library.Maven != null) {
            return new MavenLibrary(getLibrariesClient(),
                    libraryStatus.Library.Maven.Coordinates,
                    libraryStatus.Library.Maven.Repo,
                    libraryStatus.Library.Maven.Exclusions);
        } else if (libraryStatus.Library.PyPi != null) {
            return new PyPiLibrary(getLibrariesClient(),
                    libraryStatus.Library.PyPi.Package,
                    libraryStatus.Library.PyPi.Repo);
        } else if (libraryStatus.Library.Cran != null) {
            return new CranLibrary(getLibrariesClient(),
                    libraryStatus.Library.Cran.Package,
                    libraryStatus.Library.Cran.Repo);
        } else {
            throw new LibraryConfigException("Unknown Library Type");
        }
    }
}
