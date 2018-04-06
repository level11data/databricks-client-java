package com.level11data.databricks.client;

import com.level11data.databricks.client.entities.dbfs.FileInfoDTO;
import com.level11data.databricks.client.entities.dbfs.ListResponseDTO;
import com.level11data.databricks.cluster.*;
import com.level11data.databricks.cluster.builder.InteractiveClusterBuilder;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.client.entities.clusters.*;
import com.level11data.databricks.dbfs.DbfsHelper;
import com.level11data.databricks.dbfs.DbfsFileInfo;
import com.level11data.databricks.job.*;
import com.level11data.databricks.job.builder.AutomatedJarJobBuilder;
import com.level11data.databricks.job.builder.AutomatedNotebookJobBuilder;
import com.level11data.databricks.job.builder.AutomatedPythonJobBuilder;
import com.level11data.databricks.job.run.InteractiveNotebookJobRun;
import com.level11data.databricks.job.run.JobRun;
import com.level11data.databricks.library.*;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.workspace.Notebook;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class DatabricksSession {
    protected final HttpAuthenticationFeature Authentication;
    protected final URI Url;

    private final DatabricksClientConfiguration _databricksClientConfig;
    private ClustersClient _clustersClient;
    private JobsClient _jobsClient;
    private LibrariesClient _librariesClient;
    private DbfsClient _dbfsClient;

    private SparkVersionsDTO _sparkVersionsDTO;
    private NodeTypesDTO _nodeTypesDTO;
    private List<SparkVersion> _sparkVersions;
    private List<NodeType> _nodeTypes;

    public DatabricksSession(DatabricksClientConfiguration databricksConfig) {
        _databricksClientConfig = databricksConfig;

        Authentication = HttpAuthenticationFeature.basicBuilder()
                .credentials(databricksConfig.getClientUsername(), databricksConfig.getClientPassword())
                .build();

        Url = databricksConfig.getClientUrl();
    }

    public ClustersClient getClustersClient() {
        if(_clustersClient == null) {
            _clustersClient =  new ClustersClient(this);
        }
        return _clustersClient;
    }

    public LibrariesClient getLibrariesClient() {
        if(_librariesClient == null) {
            _librariesClient =  new LibrariesClient(this);
        }
        return _librariesClient;
    }

    public JobsClient getJobsClient() {
        if(_jobsClient == null) {
            _jobsClient = new JobsClient(this);
        }
        return _jobsClient;
    }

    public DbfsClient getDbfsClient() {
        if(_dbfsClient == null) {
            _dbfsClient = new DbfsClient(this);
        }
        return _dbfsClient;
    }

    public InteractiveClusterBuilder createCluster(String name, Integer numWorkers)  {
        return new InteractiveClusterBuilder(getClustersClient(), name, numWorkers);
    }

    public InteractiveClusterBuilder createCluster(String name, Integer minWorkers, Integer maxWorkers) {
        return new InteractiveClusterBuilder(getClustersClient(), name, minWorkers, maxWorkers);
    }

    private void refreshSparkVersionsDTO() throws HttpException {
        _sparkVersionsDTO = getClustersClient().getSparkVersions();
    }

    private SparkVersionsDTO getOrRequestSparkVersionsDTO() throws HttpException {
        if(_sparkVersionsDTO == null) {
            refreshSparkVersionsDTO();
        }
        return _sparkVersionsDTO;
    }

    public SparkVersion getDefaultSparkVersion() throws HttpException, ClusterConfigException  {
        return getSparkVersionByKey(getOrRequestSparkVersionsDTO().DefaultVersionKey);
    }

    private void initSparkVersions() throws HttpException {
        String defaultSparkVersionKey = getOrRequestSparkVersionsDTO().DefaultVersionKey;
        boolean isDefaultKeyInList = false;

        List<SparkVersionDTO> sparkVersionsDTO = getOrRequestSparkVersionsDTO().Versions;
        ArrayList<SparkVersion> sparkVersions = new ArrayList<SparkVersion>();

        for(SparkVersionDTO svDTO : sparkVersionsDTO) {
            sparkVersions.add(new SparkVersion(svDTO.Key, svDTO.Name));
            if(svDTO.Key.equals(defaultSparkVersionKey)) {
                isDefaultKeyInList = true;
            }
        }
        //It's possible that the Default Spark Version is not included in the list
        // possibly because it is deprecated.  If so, add it to the list with the key
        // as both the key and the value (since the value cannot be derived)
        if(!isDefaultKeyInList) {
            sparkVersions.add(new SparkVersion(defaultSparkVersionKey, defaultSparkVersionKey));
        }
        _sparkVersions = sparkVersions;
    }

    public List<SparkVersion> getSparkVersions() throws HttpException  {
        if(_sparkVersions == null) {
            initSparkVersions();
        }
        return _sparkVersions;
    }

    public SparkVersion getSparkVersionByKey(String key) throws ClusterConfigException {
        try {
            List<SparkVersion> sparkVersions = getSparkVersions();
            for (SparkVersion sv : sparkVersions) {
                if(sv.Key.equals(key)) {
                    return sv;
                }
            }
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
        throw new ClusterConfigException("No SparkVersion Found For Key "+key);
    }

    private void refreshNodeTypesDTO() throws HttpException {
        _nodeTypesDTO = getClustersClient().getNodeTypes();
    }

    private NodeTypesDTO getOrRequestNodeTypesDTO() throws HttpException {
        if(_nodeTypesDTO == null) {
            refreshNodeTypesDTO();
        }
        return _nodeTypesDTO;
    }

    public NodeType getDefaultNodeType() throws HttpException, ClusterConfigException {
        return getNodeTypeById(getOrRequestNodeTypesDTO().DefaultNodeTypeId);
    }

    private void initNodeTypes() throws ClusterConfigException {
        try {
            List<NodeTypeDTO> nodeTypesInfoListDTO
                    = getClustersClient().getNodeTypes().NodeTypes;

            ArrayList<NodeType> nodeTypesList = new ArrayList<NodeType>();

            for(NodeTypeDTO nt : nodeTypesInfoListDTO) {
                nodeTypesList.add(new NodeType(nt));
            }
            _nodeTypes = nodeTypesList;
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public List<NodeType> getNodeTypes() throws ClusterConfigException  {
        if(_nodeTypes == null) {
            initNodeTypes();
        }
        return _nodeTypes;
    }

    public NodeType getNodeTypeById(String id) throws ClusterConfigException {
        List<NodeType> nodeTypes = getNodeTypes();

        for (NodeType nt : nodeTypes) {
            if(nt.Id.equals(id)) {
                return nt;
            }
        }

        //No NodeTypeDTO found
        throw new ClusterConfigException("No NodeType Found For Id "+id);
    }

    public String getDefaultZone()  throws HttpException {
        return getClustersClient().getZones().DefaultZone;
    }

    public String[] getZones() throws HttpException  {
        return getClustersClient().getZones().Zones;
    }

    public Iterator<InteractiveCluster> listClusters() throws HttpException {
        ClustersClient client = getClustersClient();
        ClusterInfoDTO[] clusterInfoDTOs = client.listClusters().Clusters;
        return new ClusterIter(client, clusterInfoDTOs);
    }

    public InteractiveCluster getCluster(String id) throws ClusterConfigException {
        try {
            ClustersClient client = getClustersClient();
            ClusterInfoDTO clusterInfoDTO = client.getCluster(id);
            return new InteractiveCluster(client, clusterInfoDTO);
        } catch(HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public Job getJob(long jobId) throws JobConfigException {
        try {
            JobsClient client = getJobsClient();
            JobDTO jobDTO = client.getJob(jobId);

            if(jobDTO.isInteractive() && jobDTO.isNotebookJob()) {
                InteractiveCluster cluster = getCluster(jobDTO.Settings.ExistingClusterId);
                Notebook notebook = new Notebook(jobDTO.Settings.NotebookTask.NotebookPath);
                return new InteractiveNotebookJob(client, cluster, jobDTO.Settings,notebook);
            } else if(jobDTO.isAutomated() && jobDTO.isNotebookJob()) {
                return new AutomatedNotebookJob(client, jobDTO);
            } else if(jobDTO.isInteractive() && jobDTO.isJarJob()) {
                InteractiveCluster cluster = getCluster(jobDTO.Settings.ExistingClusterId);
                return new InteractiveJarJob(client,cluster, jobDTO.Settings);
            } else if(jobDTO.isAutomated() && jobDTO.isJarJob()) {
                return new AutomatedJarJob(client, jobDTO.Settings);
            } else if(jobDTO.isInteractive() && jobDTO.isPythonJob()) {
                InteractiveCluster cluster = getCluster(jobDTO.Settings.ExistingClusterId);
                PythonScript pythonScript = getPythonScript(new URI(jobDTO.Settings.SparkPythonTask.PythonFile));
                return new InteractivePythonJob(client, cluster, pythonScript, jobDTO.Settings);
            } else if(jobDTO.isAutomated() && jobDTO.isPythonJob()) {
                PythonScript pythonScript = getPythonScript(new URI(jobDTO.Settings.SparkPythonTask.PythonFile));
                return new AutomatedPythonJob(client, pythonScript, jobDTO.Settings);
            } else if(jobDTO.isAutomated() && jobDTO.isSparkSubmitJob()) {
                //TODO implement SparkSubmitJob type
            }
        } catch(HttpException e) {
            throw new JobConfigException(e);
        } catch(ClusterConfigException e) {
            throw new JobConfigException(e);
        } catch(URISyntaxException e) {
            throw new JobConfigException(e);
        } catch(ResourceConfigException e) {
            throw new JobConfigException(e);
        }
        //No valid Job Type was found
        throw new JobConfigException("Unsupported Job Type");
    }

    public JobRun getRun(long runId) throws HttpException, Exception {
        JobsClient client = getJobsClient();
        RunDTO runDTO = client.getRun(runId);

        if(runDTO.isInteractive() && runDTO.isNotebookJob()) {
            JobRun run = new InteractiveNotebookJobRun(client, runDTO);
            return run;
        } else {
            throw new Exception("Unsupported Job Type");  //TODO add better exception handling
        }
    }

    public AutomatedNotebookJobBuilder createJob(Notebook notebook) {
        return new AutomatedNotebookJobBuilder(getJobsClient(), notebook);
    }

    public AutomatedNotebookJobBuilder createJob(Notebook notebook, Map<String,String> parameters) {
        return new AutomatedNotebookJobBuilder(getJobsClient(), notebook, parameters);
    }

    public AutomatedJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName, List<String> parameters) {
        return new AutomatedJarJobBuilder(getJobsClient(), mainClassName, jarLibrary, parameters);
    }

    public AutomatedJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName) {
        return new AutomatedJarJobBuilder(getJobsClient(), mainClassName, jarLibrary);
    }

    public AutomatedJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName, File jarFile) {
        return new AutomatedJarJobBuilder(getJobsClient(), mainClassName, jarLibrary, jarFile);
    }

    public AutomatedJarJobBuilder createJob(JarLibrary jarLibrary, String mainClassName, File jarFile, List<String> parameters) {
        return new AutomatedJarJobBuilder(getJobsClient(), mainClassName, jarLibrary, jarFile, parameters);
    }

    public AutomatedPythonJobBuilder createJob(PythonScript pythonScript, File pythonFile, List<String> parameters) {
        return new AutomatedPythonJobBuilder(getJobsClient(), pythonScript, pythonFile, parameters);
    }

    public AutomatedPythonJobBuilder createJob(PythonScript pythonScript, File pythonFile) {
        return new AutomatedPythonJobBuilder(getJobsClient(), pythonScript, pythonFile);
    }

    public AutomatedPythonJobBuilder createJob(PythonScript pythonScript) {
        return new AutomatedPythonJobBuilder(getJobsClient(), pythonScript);
    }

    public AutomatedPythonJobBuilder createJob(PythonScript pythonScript, List<String> parameters) {
        return new AutomatedPythonJobBuilder(getJobsClient(), pythonScript, parameters);
    }

    public void putDbfsFile(File file, String dbfsPath,boolean overwrite) throws FileNotFoundException, IOException, HttpException {
        DbfsHelper.putFile(getDbfsClient(), file, dbfsPath, overwrite);
    }

    public void putDbfsFile(File file, String dbfsPath) throws FileNotFoundException, IOException, HttpException {
        DbfsHelper.putFile(getDbfsClient(), file, dbfsPath);
    }

    public byte[] getDbfsObject(String dbfsPath) throws IOException, HttpException {
        return DbfsHelper.getObject(getDbfsClient(), dbfsPath);
    }

    public DbfsFileInfo getDbfsObjectStatus(String dbfsPath) throws HttpException {
        return new DbfsFileInfo(getDbfsClient().getStatus(dbfsPath));
    }

    public void deleteDbfsObject(String dbfsPath, boolean recursive) throws HttpException {
        getDbfsClient().delete(dbfsPath, recursive);
    }

    public void moveDbfsObject(String fromPath, String toPath) throws HttpException {
        getDbfsClient().move(fromPath, toPath);
    }

    public void mkdirsDbfs(String path) throws HttpException {
        getDbfsClient().mkdirs(path);
    }

    //TODO add listDbfs(String path) and return iterator

    public ArrayList<DbfsFileInfo> listDbfs(String path) throws HttpException {
        ListResponseDTO listResponseDTO = getDbfsClient().list(path);
        ArrayList<DbfsFileInfo> fileList = new ArrayList<DbfsFileInfo>();

        for (FileInfoDTO fileInfo : listResponseDTO.Files) {
            fileList.add(new DbfsFileInfo(fileInfo));
        }

        return fileList;
    }

    public JarLibrary getJarLibrary(URI uri) throws LibraryConfigException {
        return new JarLibrary(getLibrariesClient(), uri);
    }

    public EggLibrary getEggLibrary(URI uri) throws LibraryConfigException {
        return new EggLibrary(getLibrariesClient(), uri);
    }

    public MavenLibrary getMavenLibrary(String coordinates) throws LibraryConfigException {
        return new MavenLibrary(getLibrariesClient(), coordinates);
    }

    public MavenLibrary getMavenLibrary(String coordinates,
                                        String repo) throws LibraryConfigException {
        return new MavenLibrary(getLibrariesClient(), coordinates, repo);
    }

    public MavenLibrary getMavenLibrary(String coordinates,
                                        String repo,
                                        String[] exclusions) throws LibraryConfigException {
        return new MavenLibrary(getLibrariesClient(), coordinates, repo, exclusions);
    }

    public MavenLibrary getMavenLibrary(String coordinates,
                                        String[] exclusions) throws LibraryConfigException {
        return new MavenLibrary(getLibrariesClient(), coordinates, exclusions);
    }

    public PyPiLibrary getPyPiLibrary(String packageName) throws LibraryConfigException {
        return new PyPiLibrary(getLibrariesClient(), packageName);
    }

    public PyPiLibrary getPyPiLibrary(String packageName, String repo) throws LibraryConfigException {
        return new PyPiLibrary(getLibrariesClient(), packageName, repo);
    }

    public CranLibrary getCranLibrary(String packageName) throws LibraryConfigException {
        return new CranLibrary(getLibrariesClient(), packageName);
    }

    public CranLibrary getCranLibrary(String packageName, String repo) throws LibraryConfigException {
        return new CranLibrary(getLibrariesClient(), packageName, repo);
    }

    public PythonScript getPythonScript(URI uri) throws ResourceConfigException {
        return new PythonScript(this, uri);
    }

}

