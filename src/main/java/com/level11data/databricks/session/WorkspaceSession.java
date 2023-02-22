package com.level11data.databricks.session;

import com.level11data.databricks.client.entities.instancepools.InstancePoolGetResponseDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolListResponseDTO;
import com.level11data.databricks.instancepool.InstancePoolConfigException;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.builder.CreateInstancePoolBuilder;
import com.level11data.databricks.workspace.builder.ScalaNotebookBuilder;
import com.level11data.databricks.client.entities.workspace.WorkspaceDeleteRequestDTO;
import com.level11data.databricks.client.entities.workspace.WorkspaceMkdirsRequestDTO;
import com.level11data.databricks.library.CranLibrary;
import com.level11data.databricks.library.PyPiLibrary;
import com.level11data.databricks.library.MavenLibrary;
import com.level11data.databricks.library.EggLibrary;
import com.level11data.databricks.library.LibraryConfigException;
import com.level11data.databricks.client.entities.dbfs.FileInfoDTO;
import com.level11data.databricks.client.entities.dbfs.DbfsListResponseDTO;
import com.level11data.databricks.client.entities.dbfs.DbfsDeleteRequestDTO;
import com.level11data.databricks.dbfs.DbfsFileInfo;
import com.level11data.databricks.dbfs.DbfsException;
import com.level11data.databricks.dbfs.DbfsHelper;
import com.level11data.databricks.job.builder.AutomatedSparkSubmitJobBuilder;
import com.level11data.databricks.job.builder.AutomatedPythonJobBuilder;
import java.io.File;
import com.level11data.databricks.job.builder.AutomatedJarJobBuilder;
import com.level11data.databricks.library.JarLibrary;
import com.level11data.databricks.job.builder.AutomatedNotebookJobBuilder;
import com.level11data.databricks.client.entities.jobs.RunDTO;
import com.level11data.databricks.job.run.JobRunException;
import com.level11data.databricks.job.run.AutomatedSparkSubmitJobRun;
import com.level11data.databricks.job.run.InteractivePythonJobRun;
import com.level11data.databricks.job.run.AutomatedPythonJobRun;
import com.level11data.databricks.job.run.InteractiveJarJobRun;
import com.level11data.databricks.job.run.AutomatedJarJobRun;
import com.level11data.databricks.job.run.AutomatedNotebookJobRun;
import com.level11data.databricks.job.run.InteractiveNotebookJobRun;
import com.level11data.databricks.job.run.JobRun;
import com.level11data.databricks.job.PythonScript;
import com.level11data.databricks.workspace.Notebook;
import com.level11data.databricks.client.entities.jobs.JobDTO;
import com.level11data.databricks.workspace.WorkspaceConfigException;
import com.level11data.databricks.util.ResourceConfigException;
import java.net.URISyntaxException;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.job.AutomatedSparkSubmitJob;
import com.level11data.databricks.job.AutomatedPythonJob;
import com.level11data.databricks.job.InteractivePythonJob;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.InteractiveJarJob;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.Job;
import com.level11data.databricks.client.entities.clusters.ClusterInfoDTO;
import com.level11data.databricks.cluster.ClusterIter;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.client.entities.clusters.NodeTypeDTO;
import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.client.entities.clusters.SparkVersionDTO;
import java.util.ArrayList;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;
import com.level11data.databricks.cluster.builder.InteractiveClusterBuilder;
import java.util.Iterator;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Invocation;
import javax.net.ssl.SSLContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.client.ClientBuilder;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.spi.ConnectorProvider;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.client.ClientConfig;
import com.level11data.databricks.config.DatabricksClientConfigException;
import com.level11data.databricks.workspace.util.WorkspaceHelper;
import com.level11data.databricks.cluster.NodeType;
import com.level11data.databricks.cluster.SparkVersion;
import java.util.List;
import com.level11data.databricks.client.entities.clusters.NodeTypesDTO;
import com.level11data.databricks.client.entities.clusters.SparkVersionsDTO;
import com.level11data.databricks.client.InstancePoolsClient;
import com.level11data.databricks.client.WorkspaceClient;
import com.level11data.databricks.client.DbfsClient;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.JobsClient;
import com.level11data.databricks.client.ClustersClient;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import javax.ws.rs.client.Client;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import java.net.URI;

public class WorkspaceSession
{
    protected final URI Endpoint;
    private final DatabricksClientConfiguration _databricksClientConfig;
    private Client _httpClient;
    private HttpAuthenticationFeature _userPassAuth;
    private ClustersClient _clustersClient;
    private JobsClient _jobsClient;
    private LibrariesClient _librariesClient;
    private DbfsClient _dbfsClient;
    private WorkspaceClient _workspaceClient;
    private InstancePoolsClient _instancePoolsClient;
    private SparkVersionsDTO _sparkVersionsDTO;
    private NodeTypesDTO _nodeTypesDTO;
    private List<SparkVersion> _sparkVersions;
    private List<NodeType> _nodeTypes;
    private WorkspaceHelper _workspaceHelper;
    private final String SECURITY_PROTOCOL = "TLSv1.2";

    public WorkspaceSession(DatabricksClientConfiguration databricksClientConfig) throws DatabricksClientConfigException {
        _databricksClientConfig = databricksClientConfig;
        Endpoint = databricksClientConfig.getWorkspaceUrl();
        createEncryptedHttpSession();
    }

    public WorkspaceSession(URI workspaceUrl, String token) throws DatabricksClientConfigException {
        this(new DatabricksClientConfiguration(workspaceUrl, token));
    }

    public WorkspaceSession() throws DatabricksClientConfigException {
        this(new DatabricksClientConfiguration());
    }

    private void createEncryptedHttpSession() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register((Object)new JacksonFeature());
        clientConfig.connectorProvider((ConnectorProvider)new HttpUrlConnectorProvider());
        SSLContext sslContext = SslConfigurator.newInstance().securityProtocol("TLSv1.2").createSSLContext();
        System.setProperty("https.protocols", "TLSv1.2");
        System.setProperty("jdk.tls.client.protocols", "TLSv1.2");
        _httpClient = ClientBuilder.newBuilder().sslContext(sslContext).withConfig((Configuration)clientConfig).build();
    }

    public Invocation.Builder getRequestBuilder(String path) {
        return getRequestBuilder(path, null);
    }

    public Invocation.Builder getRequestBuilder(String path, String queryParamKey, Object queryParamValue) {
        Map<String, Object> queryMap = new HashMap<String, Object>();
        queryMap.put(queryParamKey, queryParamValue);
        return this.getRequestBuilder(path, queryMap);
    }

    public Invocation.Builder getRequestBuilder(String path, Map<String, Object> queryParams) {
        if (_databricksClientConfig.hasClientToken()) {
            if (queryParams != null) {
                WebTarget target = _httpClient.target(this.Endpoint).path(path);
                target = applyQueryParameters(target, queryParams);
                return target.request(MediaType.APPLICATION_JSON_TYPE).header("Authorization", "Bearer " + this._databricksClientConfig.getWorkspaceToken()).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept(MediaType.APPLICATION_JSON);
            }
            return _httpClient.target(this.Endpoint).path(path).request(MediaType.APPLICATION_JSON_TYPE).header("Authorization", "Bearer " + this._databricksClientConfig.getWorkspaceToken()).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept(MediaType.APPLICATION_JSON);
        }
        else {
            if (queryParams != null) {
                WebTarget target = this._httpClient.target(this.Endpoint).path(path);
                target = this.applyQueryParameters(target, queryParams);
                return target.request(MediaType.APPLICATION_JSON_TYPE).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept(MediaType.APPLICATION_JSON);
            }
            return _httpClient.target(this.Endpoint).path(path).register(this.getUserPassAuth()).request(MediaType.APPLICATION_JSON_TYPE).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept(MediaType.APPLICATION_JSON);
        }
    }

    private WebTarget applyQueryParameters(WebTarget target, final Map<String, Object> queryParams) {
        for (Map.Entry<String, Object> queryParam : queryParams.entrySet()) {
            target = target.queryParam(queryParam.getKey(), queryParam.getValue());
        }
        return target;
    }

    private HttpAuthenticationFeature getUserPassAuth() {
        if (_userPassAuth == null) {
            _userPassAuth = HttpAuthenticationFeature.basicBuilder().credentials(_databricksClientConfig.getWorkspaceUsername(), _databricksClientConfig.getWorkspacePassword()).build();
        }
        return _userPassAuth;
    }

    public ClustersClient getClustersClient() {
        if (_clustersClient == null) {
            _clustersClient = new ClustersClient(this);
        }
        return _clustersClient;
    }

    public LibrariesClient getLibrariesClient() {
        if (_librariesClient == null) {
            _librariesClient = new LibrariesClient(this);
        }
        return _librariesClient;
    }

    public JobsClient getJobsClient() {
        if (_jobsClient == null) {
            _jobsClient = new JobsClient(this);
        }
        return _jobsClient;
    }

    public DbfsClient getDbfsClient() {
        if (_dbfsClient == null) {
            _dbfsClient = new DbfsClient(this);
        }
        return _dbfsClient;
    }

    public WorkspaceClient getWorkspaceClient() {
        if (_workspaceClient == null) {
            _workspaceClient = new WorkspaceClient(this);
        }
        return _workspaceClient;
    }

    public InstancePoolsClient getInstancePoolsClient() {
        if (_instancePoolsClient == null) {
            _instancePoolsClient = new InstancePoolsClient(this);
        }
        return _instancePoolsClient;
    }

    public InteractiveClusterBuilder createInteractiveCluster(String name, Integer numWorkers) {
        return new InteractiveClusterBuilder(getClustersClient(), name, numWorkers);
    }

    public InteractiveClusterBuilder createInteractiveCluster(String name, Integer minWorkers, Integer maxWorkers) {
        return new InteractiveClusterBuilder(getClustersClient(), name, minWorkers, maxWorkers);
    }

    public AutomatedClusterBuilder createClusterSpec(Integer numWorkers) {
        return new AutomatedClusterBuilder(getClustersClient(), numWorkers);
    }

    public AutomatedClusterBuilder createClusterSpec(Integer minWorkers, Integer maxWorkers) {
        return new AutomatedClusterBuilder(getClustersClient(), minWorkers, maxWorkers);
    }

    private void refreshSparkVersionsDTO() throws HttpException {
        _sparkVersionsDTO = getClustersClient().getSparkVersions();
    }

    private SparkVersionsDTO getOrRequestSparkVersionsDTO() throws HttpException {
        if (_sparkVersionsDTO == null) {
            refreshSparkVersionsDTO();
        }
        return _sparkVersionsDTO;
    }

    private void initSparkVersions() throws HttpException {
        List<SparkVersionDTO> sparkVersionsDTO = this.getOrRequestSparkVersionsDTO().Versions;
        ArrayList<SparkVersion> sparkVersions = new ArrayList<SparkVersion>();
        for (SparkVersionDTO svDTO : sparkVersionsDTO) {
            sparkVersions.add(new SparkVersion(svDTO.Key, svDTO.Name));
        }
        _sparkVersions = sparkVersions;
    }

    public List<SparkVersion> getSparkVersions() throws HttpException {
        if (_sparkVersions == null) {
            initSparkVersions();
        }
        return _sparkVersions;
    }

    public SparkVersion getSparkVersionByKey(String key) throws ClusterConfigException {
        try {
            List<SparkVersion> sparkVersions = this.getSparkVersions();
            for (SparkVersion sv : sparkVersions) {
                if (sv.Key.equals(key)) {
                    return sv;
                }
            }
        }
        catch (HttpException e) {
            throw new ClusterConfigException(e);
        }
        throw new ClusterConfigException("No SparkVersion Found For Key " + key);
    }

    private void initNodeTypes() throws ClusterConfigException {
        try {
            List<NodeTypeDTO> nodeTypesInfoListDTO = this.getClustersClient().getNodeTypes().NodeTypes;
            ArrayList<NodeType> nodeTypesList = new ArrayList<NodeType>();
            for (NodeTypeDTO nt : nodeTypesInfoListDTO) {
                nodeTypesList.add(new NodeType(nt));
            }
            _nodeTypes = nodeTypesList;
        }
        catch (HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public List<NodeType> getNodeTypes() throws ClusterConfigException {
        if (_nodeTypes == null) {
            initNodeTypes();
        }
        return _nodeTypes;
    }

    private WorkspaceHelper getWorkspaceHelper() {
        if (_workspaceHelper == null) {
            _workspaceHelper = new WorkspaceHelper(getWorkspaceClient());
        }
        return _workspaceHelper;
    }

    public NodeType getNodeTypeById(final String id) throws ClusterConfigException {
        List<NodeType> nodeTypes = getNodeTypes();
        for (NodeType nt : nodeTypes) {
            if (nt.Id.equals(id)) {
                return nt;
            }
        }
        throw new ClusterConfigException("No NodeType Found For Id " + id);
    }

    public String getDefaultZone() throws HttpException {
        return getClustersClient().getZones().DefaultZone;
    }

    public String[] getZones() throws HttpException {
        return getClustersClient().getZones().Zones;
    }

    public Iterator<InteractiveCluster> listClusters() throws HttpException {
        final ClustersClient client = this.getClustersClient();
        final ClusterInfoDTO[] clusterInfoDTOs = client.listClusters().Clusters;
        return new ClusterIter(client, clusterInfoDTOs);
    }

    public InteractiveCluster getCluster(final String id) throws ClusterConfigException {
        try {
            ClustersClient client = this.getClustersClient();
            ClusterInfoDTO clusterInfoDTO = client.getCluster(id);
            return new InteractiveCluster(client, clusterInfoDTO);
        }
        catch (HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public Job getJob(final long jobId) throws JobConfigException {
        final JobsClient client = this.getJobsClient();
        try {
            JobDTO jobDTO = client.getJob(jobId);
            if (jobDTO.isInteractive() && jobDTO.isNotebookJob()) {
                InteractiveCluster cluster = getCluster(jobDTO.Settings.ExistingClusterId);
                Notebook notebook = getNotebook(jobDTO.Settings.NotebookTask.NotebookPath);
                return new InteractiveNotebookJob(client, cluster, jobDTO, notebook);
            }
            if (jobDTO.isAutomated() && jobDTO.isNotebookJob()) {
                return new AutomatedNotebookJob(client, jobDTO);
            }
            if (jobDTO.isInteractive() && jobDTO.isJarJob()) {
                InteractiveCluster cluster = this.getCluster(jobDTO.Settings.ExistingClusterId);
                return new InteractiveJarJob(client, cluster, jobDTO);
            }
            if (jobDTO.isAutomated() && jobDTO.isJarJob()) {
                return new AutomatedJarJob(client, jobDTO);
            }
            if (jobDTO.isInteractive() && jobDTO.isPythonJob()) {
                InteractiveCluster cluster = getCluster(jobDTO.Settings.ExistingClusterId);
                PythonScript pythonScript = getPythonScript(new URI(jobDTO.Settings.SparkPythonTask.PythonFile));
                return new InteractivePythonJob(client, cluster, pythonScript, jobDTO);
            }
            if (jobDTO.isAutomated() && jobDTO.isPythonJob()) {
                PythonScript pythonScript2 = this.getPythonScript(new URI(jobDTO.Settings.SparkPythonTask.PythonFile));
                return new AutomatedPythonJob(client, pythonScript2, jobDTO);
            }
            if (jobDTO.isAutomated() && jobDTO.isSparkSubmitJob()) {
                return new AutomatedSparkSubmitJob(client, jobDTO);
            }
        }
        catch (HttpException e) {
            throw new JobConfigException(e);
        }
        catch (ClusterConfigException e2) {
            throw new JobConfigException(e2);
        }
        catch (URISyntaxException e3) {
            throw new JobConfigException(e3);
        }
        catch (ResourceConfigException e4) {
            throw new JobConfigException(e4);
        }
        catch (WorkspaceConfigException e5) {
            try {
                JobDTO jobDTO2 = client.getJob(jobId);
                throw new JobConfigException("Notebook Path " + jobDTO2.Settings.NotebookTask.NotebookPath + " does not exist for Job " + jobDTO2.Settings.Name, e5);
            }
            catch (HttpException he) {
                throw new JobConfigException(e5);
            }
        }
        throw new JobConfigException("Unsupported Job Type");
    }

    public Job getFirstJobByName(final String jobName) throws JobConfigException {
        final JobsClient client = this.getJobsClient();
        try {
            JobDTO[] jobs;
            JobDTO[] jobsDTO = jobs = client.listJobs().Jobs;
            for (final JobDTO jobDTO : jobs) {
                if (jobDTO.Settings.Name.equals(jobName)) {
                    return getJob(jobDTO.JobId);
                }
            }
            return null;
        }
        catch (HttpException e) {
            throw new JobConfigException(e);
        }
    }

    public JobRun getRun(long runId) throws JobRunException {
        try {
            JobsClient client = getJobsClient();
            RunDTO runDTO = client.getRun(runId);
            if (runDTO.isInteractive() && runDTO.isNotebookJob()) {
                return new InteractiveNotebookJobRun(client, runDTO);
            }
            if (runDTO.isAutomated() && runDTO.isNotebookJob()) {
                return new AutomatedNotebookJobRun(client, runDTO);
            }
            if (runDTO.isAutomated() && runDTO.isJarJob()) {
                return new AutomatedJarJobRun(client, runDTO);
            }
            if (runDTO.isInteractive() && runDTO.isJarJob()) {
                return new InteractiveJarJobRun(client, runDTO);
            }
            if (runDTO.isAutomated() && runDTO.isPythonJob()) {
                final PythonScript pyScript = new PythonScript(this, new URI(runDTO.Task.SparkPythonTask.PythonFile));
                return new AutomatedPythonJobRun(client, pyScript, runDTO);
            }
            if (runDTO.isInteractive() && runDTO.isPythonJob()) {
                final PythonScript pyScript = new PythonScript(this, new URI(runDTO.Task.SparkPythonTask.PythonFile));
                return new InteractivePythonJobRun(client, pyScript, runDTO);
            }
            if (runDTO.isSparkSubmitJob()) {
                return new AutomatedSparkSubmitJobRun(client, runDTO);
            }
            throw new JobRunException("Unsupported Job Type");
        }
        catch (HttpException e) {
            throw new JobRunException(e);
        }
        catch (ResourceConfigException e2) {
            throw new JobRunException(e2);
        }
        catch (URISyntaxException e3) {
            throw new JobRunException(e3);
        }
    }

    public AutomatedNotebookJobBuilder createJob(Notebook notebook) {
        return new AutomatedNotebookJobBuilder(getJobsClient(), notebook);
    }

    public AutomatedNotebookJobBuilder createJob(Notebook notebook, final Map<String, String> parameters) {
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

    public AutomatedSparkSubmitJobBuilder createJob(List<String> parameters) {
        return new AutomatedSparkSubmitJobBuilder(getJobsClient(), parameters);
    }

    public void putDbfsFile(File file, String dbfsPath, boolean overwrite) throws DbfsException {
        DbfsHelper.putFile(getDbfsClient(), file, dbfsPath, overwrite);
    }

    public void putDbfsFile(File file, String dbfsPath) throws DbfsException {
        DbfsHelper.putFile(getDbfsClient(), file, dbfsPath);
    }

    public byte[] getDbfsObject(String dbfsPath) throws DbfsException {
        return DbfsHelper.getObject(getDbfsClient(), dbfsPath);
    }

    public DbfsFileInfo getDbfsObjectStatus(String dbfsPath) throws HttpException {
        return new DbfsFileInfo(getDbfsClient().getStatus(dbfsPath));
    }

    public void deleteDbfsObject(String dbfsPath, boolean recursive) throws HttpException {
        final DbfsDeleteRequestDTO dbfsDeleteRequestDTO = new DbfsDeleteRequestDTO();
        dbfsDeleteRequestDTO.Path = dbfsPath;
        dbfsDeleteRequestDTO.Recursive = recursive;
        getDbfsClient().delete(dbfsDeleteRequestDTO);
    }

    public void moveDbfsObject(String fromPath, String toPath) throws HttpException {
        getDbfsClient().move(fromPath, toPath);
    }

    public void mkdirsDbfs(String path) throws HttpException {
        getDbfsClient().mkdirs(path);
    }

    public ArrayList<DbfsFileInfo> listDbfs(String path) throws HttpException {
        DbfsListResponseDTO dbfsListResponseDTO = getDbfsClient().list(path);
        ArrayList<DbfsFileInfo> fileList = new ArrayList<DbfsFileInfo>();
        for (final FileInfoDTO fileInfo : dbfsListResponseDTO.Files) {
            fileList.add(new DbfsFileInfo(fileInfo));
        }
        return fileList;
    }

    public JarLibrary getJarLibrary(URI uri) throws LibraryConfigException {
        return new JarLibrary(this.getLibrariesClient(), uri);
    }

    public EggLibrary getEggLibrary(URI uri) throws LibraryConfigException {
        return new EggLibrary(this.getLibrariesClient(), uri);
    }

    public MavenLibrary getMavenLibrary(String coordinates) throws LibraryConfigException {
        return new MavenLibrary(this.getLibrariesClient(), coordinates);
    }

    public MavenLibrary getMavenLibrary(String coordinates, String repo) throws LibraryConfigException {
        return new MavenLibrary(getLibrariesClient(), coordinates, repo);
    }

    public MavenLibrary getMavenLibrary(String coordinates, String repo, String[] exclusions) throws LibraryConfigException {
        return new MavenLibrary(getLibrariesClient(), coordinates, repo, exclusions);
    }

    public MavenLibrary getMavenLibrary(String coordinates, String[] exclusions) throws LibraryConfigException {
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

    public CranLibrary getCranLibrary(String packageName, final String repo) throws LibraryConfigException {
        return new CranLibrary(getLibrariesClient(), packageName, repo);
    }

    public PythonScript getPythonScript(URI uri) throws ResourceConfigException {
        return new PythonScript(this, uri);
    }

    public void mkdirsWorkspace(String workspacePath) throws WorkspaceConfigException {
        try {
            WorkspaceMkdirsRequestDTO workspaceMkdirsRequestDTO = new WorkspaceMkdirsRequestDTO();
            workspaceMkdirsRequestDTO.Path = workspacePath;
            getWorkspaceClient().mkdirs(workspaceMkdirsRequestDTO);
        }
        catch (HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public void deleteWorkspaceObject(String workspacePath, boolean recursive) throws WorkspaceConfigException {
        try {
            WorkspaceDeleteRequestDTO workspaceDeleteRequestDTO = new WorkspaceDeleteRequestDTO();
            workspaceDeleteRequestDTO.Path = workspacePath;
            workspaceDeleteRequestDTO.Recursive = recursive;
            getWorkspaceClient().delete(workspaceDeleteRequestDTO);
        }
        catch (HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public Notebook getNotebook(String workspacePath) throws WorkspaceConfigException {
        return getWorkspaceHelper().getNotebook(workspacePath);
    }

    public ScalaNotebookBuilder createScalaNotebook() throws WorkspaceConfigException {
        return new ScalaNotebookBuilder(this.getWorkspaceClient());
    }

    public ScalaNotebookBuilder createScalaNotebook(File file) throws WorkspaceConfigException {
        return new ScalaNotebookBuilder(this.getWorkspaceClient(), file);
    }

    public CreateInstancePoolBuilder createInstancePool() {
        return new CreateInstancePoolBuilder(this.getInstancePoolsClient());
    }

    public InstancePool getInstancePool(String instancePoolId) throws InstancePoolConfigException {
        try {
            return new InstancePool(this.getInstancePoolsClient(), this.getInstancePoolsClient().getInstancePool(instancePoolId));
        }
        catch (HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

    public InstancePool getFirstInstancePoolByName(String instancePoolName) throws InstancePoolConfigException {
        try {
            InstancePoolListResponseDTO instancePoolListDTO = this.getInstancePoolsClient().listInstancePools();
            if (instancePoolListDTO != null) {
                for (InstancePoolGetResponseDTO pool : instancePoolListDTO.InstancePools) {
                    if (pool.InstancePoolName.equals(instancePoolName)) {
                        return new InstancePool(this.getInstancePoolsClient(), this.getInstancePoolsClient().getInstancePool(pool.InstancePoolId));
                    }
                }
            }
            return null;
        }
        catch (HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

    public URI getEndpoint() {
        return Endpoint;
    }

    public String getToken() {
        return _databricksClientConfig.getWorkspaceToken();
    }

    public String getUserAgent() {
        return _databricksClientConfig.getUserAgent();
    }
}