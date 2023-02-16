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

    public WorkspaceSession(final DatabricksClientConfiguration databricksClientConfig) throws DatabricksClientConfigException {
        this._databricksClientConfig = databricksClientConfig;
        this.Endpoint = databricksClientConfig.getWorkspaceUrl();
        this.createEncryptedHttpSession();
    }

    public WorkspaceSession(final URI workspaceUrl, final String token) throws DatabricksClientConfigException {
        this(new DatabricksClientConfiguration(workspaceUrl, token));
    }

    public WorkspaceSession() throws DatabricksClientConfigException {
        this(new DatabricksClientConfiguration());
    }

    private void createEncryptedHttpSession() {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register((Object)new JacksonFeature());
        clientConfig.connectorProvider((ConnectorProvider)new HttpUrlConnectorProvider());
        final SSLContext sslContext = SslConfigurator.newInstance().securityProtocol("TLSv1.2").createSSLContext();
        System.setProperty("https.protocols", "TLSv1.2");
        System.setProperty("jdk.tls.client.protocols", "TLSv1.2");
        this._httpClient = ClientBuilder.newBuilder().sslContext(sslContext).withConfig((Configuration)clientConfig).build();
    }

    public Invocation.Builder getRequestBuilder(final String path) {
        return this.getRequestBuilder(path, null);
    }

    public Invocation.Builder getRequestBuilder(final String path, final String queryParamKey, final Object queryParamValue) {
        final Map<String, Object> queryMap = new HashMap<String, Object>();
        queryMap.put(queryParamKey, queryParamValue);
        return this.getRequestBuilder(path, queryMap);
    }

    public Invocation.Builder getRequestBuilder(final String path, final Map<String, Object> queryParams) {
        if (this._databricksClientConfig.hasClientToken()) {
            if (queryParams != null) {
                WebTarget target = this._httpClient.target(this.Endpoint).path(path);
                target = this.applyQueryParameters(target, queryParams);
                return target.request(MediaType.APPLICATION_JSON_TYPE).header("Authorization", "Bearer " + this._databricksClientConfig.getWorkspaceToken()).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept("application/json");
            }
            return this._httpClient.target(this.Endpoint).path(path).request(MediaType.APPLICATION_JSON_TYPE).header("Authorization", "Bearer " + this._databricksClientConfig.getWorkspaceToken()).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept("application/json");
        }
        else {
            if (queryParams != null) {
                WebTarget target = this._httpClient.target(this.Endpoint).path(path);
                target = this.applyQueryParameters(target, queryParams);
                return target.request(MediaType.APPLICATION_JSON_TYPE).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept("application/json");
            }
            return this._httpClient.target(this.Endpoint).path(path).register(this.getUserPassAuth()).request(MediaType.APPLICATION_JSON_TYPE).header("User-Agent", this._databricksClientConfig.getUserAgent()).accept("application/json");
        }
    }

    private WebTarget applyQueryParameters(WebTarget target, final Map<String, Object> queryParams) {
        for (final Map.Entry<String, Object> queryParam : queryParams.entrySet()) {
            target = target.queryParam(queryParam.getKey(), queryParam.getValue());
        }
        return target;
    }

    private HttpAuthenticationFeature getUserPassAuth() {
        if (this._userPassAuth == null) {
            this._userPassAuth = HttpAuthenticationFeature.basicBuilder().credentials(this._databricksClientConfig.getWorkspaceUsername(), this._databricksClientConfig.getWorkspacePassword()).build();
        }
        return this._userPassAuth;
    }

    public ClustersClient getClustersClient() {
        if (this._clustersClient == null) {
            this._clustersClient = new ClustersClient(this);
        }
        return this._clustersClient;
    }

    public LibrariesClient getLibrariesClient() {
        if (this._librariesClient == null) {
            this._librariesClient = new LibrariesClient(this);
        }
        return this._librariesClient;
    }

    public JobsClient getJobsClient() {
        if (this._jobsClient == null) {
            this._jobsClient = new JobsClient(this);
        }
        return this._jobsClient;
    }

    public DbfsClient getDbfsClient() {
        if (this._dbfsClient == null) {
            this._dbfsClient = new DbfsClient(this);
        }
        return this._dbfsClient;
    }

    public WorkspaceClient getWorkspaceClient() {
        if (this._workspaceClient == null) {
            this._workspaceClient = new WorkspaceClient(this);
        }
        return this._workspaceClient;
    }

    public InstancePoolsClient getInstancePoolsClient() {
        if (this._instancePoolsClient == null) {
            this._instancePoolsClient = new InstancePoolsClient(this);
        }
        return this._instancePoolsClient;
    }

    public InteractiveClusterBuilder createInteractiveCluster(final String name, final Integer numWorkers) {
        return new InteractiveClusterBuilder(this.getClustersClient(), name, numWorkers);
    }

    public InteractiveClusterBuilder createInteractiveCluster(final String name, final Integer minWorkers, final Integer maxWorkers) {
        return new InteractiveClusterBuilder(this.getClustersClient(), name, minWorkers, maxWorkers);
    }

    public AutomatedClusterBuilder createClusterSpec(final Integer numWorkers) {
        return new AutomatedClusterBuilder(this.getClustersClient(), numWorkers);
    }

    public AutomatedClusterBuilder createClusterSpec(final Integer minWorkers, final Integer maxWorkers) {
        return new AutomatedClusterBuilder(this.getClustersClient(), minWorkers, maxWorkers);
    }

    private void refreshSparkVersionsDTO() throws HttpException {
        this._sparkVersionsDTO = this.getClustersClient().getSparkVersions();
    }

    private SparkVersionsDTO getOrRequestSparkVersionsDTO() throws HttpException {
        if (this._sparkVersionsDTO == null) {
            this.refreshSparkVersionsDTO();
        }
        return this._sparkVersionsDTO;
    }

    private void initSparkVersions() throws HttpException {
        final List<SparkVersionDTO> sparkVersionsDTO = this.getOrRequestSparkVersionsDTO().Versions;
        final ArrayList<SparkVersion> sparkVersions = new ArrayList<SparkVersion>();
        for (final SparkVersionDTO svDTO : sparkVersionsDTO) {
            sparkVersions.add(new SparkVersion(svDTO.Key, svDTO.Name));
        }
        this._sparkVersions = sparkVersions;
    }

    public List<SparkVersion> getSparkVersions() throws HttpException {
        if (this._sparkVersions == null) {
            this.initSparkVersions();
        }
        return this._sparkVersions;
    }

    public SparkVersion getSparkVersionByKey(final String key) throws ClusterConfigException {
        try {
            final List<SparkVersion> sparkVersions = this.getSparkVersions();
            for (final SparkVersion sv : sparkVersions) {
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
            final List<NodeTypeDTO> nodeTypesInfoListDTO = this.getClustersClient().getNodeTypes().NodeTypes;
            final ArrayList<NodeType> nodeTypesList = new ArrayList<NodeType>();
            for (final NodeTypeDTO nt : nodeTypesInfoListDTO) {
                nodeTypesList.add(new NodeType(nt));
            }
            this._nodeTypes = nodeTypesList;
        }
        catch (HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public List<NodeType> getNodeTypes() throws ClusterConfigException {
        if (this._nodeTypes == null) {
            this.initNodeTypes();
        }
        return this._nodeTypes;
    }

    private WorkspaceHelper getWorkspaceHelper() {
        if (this._workspaceHelper == null) {
            this._workspaceHelper = new WorkspaceHelper(this.getWorkspaceClient());
        }
        return this._workspaceHelper;
    }

    public NodeType getNodeTypeById(final String id) throws ClusterConfigException {
        final List<NodeType> nodeTypes = this.getNodeTypes();
        for (final NodeType nt : nodeTypes) {
            if (nt.Id.equals(id)) {
                return nt;
            }
        }
        throw new ClusterConfigException("No NodeType Found For Id " + id);
    }

    public String getDefaultZone() throws HttpException {
        return this.getClustersClient().getZones().DefaultZone;
    }

    public String[] getZones() throws HttpException {
        return this.getClustersClient().getZones().Zones;
    }

    public Iterator<InteractiveCluster> listClusters() throws HttpException {
        final ClustersClient client = this.getClustersClient();
        final ClusterInfoDTO[] clusterInfoDTOs = client.listClusters().Clusters;
        return new ClusterIter(client, clusterInfoDTOs);
    }

    public InteractiveCluster getCluster(final String id) throws ClusterConfigException {
        try {
            final ClustersClient client = this.getClustersClient();
            final ClusterInfoDTO clusterInfoDTO = client.getCluster(id);
            return new InteractiveCluster(client, clusterInfoDTO);
        }
        catch (HttpException e) {
            throw new ClusterConfigException(e);
        }
    }

    public Job getJob(final long jobId) throws JobConfigException {
        final JobsClient client = this.getJobsClient();
        try {
            final JobDTO jobDTO = client.getJob(jobId);
            if (jobDTO.isInteractive() && jobDTO.isNotebookJob()) {
                final InteractiveCluster cluster = this.getCluster(jobDTO.Settings.ExistingClusterId);
                final Notebook notebook = this.getNotebook(jobDTO.Settings.NotebookTask.NotebookPath);
                return new InteractiveNotebookJob(client, cluster, jobDTO, notebook);
            }
            if (jobDTO.isAutomated() && jobDTO.isNotebookJob()) {
                return new AutomatedNotebookJob(client, jobDTO);
            }
            if (jobDTO.isInteractive() && jobDTO.isJarJob()) {
                final InteractiveCluster cluster = this.getCluster(jobDTO.Settings.ExistingClusterId);
                return new InteractiveJarJob(client, cluster, jobDTO);
            }
            if (jobDTO.isAutomated() && jobDTO.isJarJob()) {
                return new AutomatedJarJob(client, jobDTO);
            }
            if (jobDTO.isInteractive() && jobDTO.isPythonJob()) {
                final InteractiveCluster cluster = this.getCluster(jobDTO.Settings.ExistingClusterId);
                final PythonScript pythonScript = this.getPythonScript(new URI(jobDTO.Settings.SparkPythonTask.PythonFile));
                return new InteractivePythonJob(client, cluster, pythonScript, jobDTO);
            }
            if (jobDTO.isAutomated() && jobDTO.isPythonJob()) {
                final PythonScript pythonScript2 = this.getPythonScript(new URI(jobDTO.Settings.SparkPythonTask.PythonFile));
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
                final JobDTO jobDTO2 = client.getJob(jobId);
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
            final JobDTO[] jobs;
            final JobDTO[] jobsDTO = jobs = client.listJobs().Jobs;
            for (final JobDTO jobDTO : jobs) {
                if (jobDTO.Settings.Name.equals(jobName)) {
                    return this.getJob(jobDTO.JobId);
                }
            }
            return null;
        }
        catch (HttpException e) {
            throw new JobConfigException(e);
        }
    }

    public JobRun getRun(final long runId) throws JobRunException {
        try {
            final JobsClient client = this.getJobsClient();
            final RunDTO runDTO = client.getRun(runId);
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

    public AutomatedNotebookJobBuilder createJob(final Notebook notebook) {
        return new AutomatedNotebookJobBuilder(this.getJobsClient(), notebook);
    }

    public AutomatedNotebookJobBuilder createJob(final Notebook notebook, final Map<String, String> parameters) {
        return new AutomatedNotebookJobBuilder(this.getJobsClient(), notebook, parameters);
    }

    public AutomatedJarJobBuilder createJob(final JarLibrary jarLibrary, final String mainClassName, final List<String> parameters) {
        return new AutomatedJarJobBuilder(this.getJobsClient(), mainClassName, jarLibrary, parameters);
    }

    public AutomatedJarJobBuilder createJob(final JarLibrary jarLibrary, final String mainClassName) {
        return new AutomatedJarJobBuilder(this.getJobsClient(), mainClassName, jarLibrary);
    }

    public AutomatedJarJobBuilder createJob(final JarLibrary jarLibrary, final String mainClassName, final File jarFile) {
        return new AutomatedJarJobBuilder(this.getJobsClient(), mainClassName, jarLibrary, jarFile);
    }

    public AutomatedJarJobBuilder createJob(final JarLibrary jarLibrary, final String mainClassName, final File jarFile, final List<String> parameters) {
        return new AutomatedJarJobBuilder(this.getJobsClient(), mainClassName, jarLibrary, jarFile, parameters);
    }

    public AutomatedPythonJobBuilder createJob(final PythonScript pythonScript, final File pythonFile, final List<String> parameters) {
        return new AutomatedPythonJobBuilder(this.getJobsClient(), pythonScript, pythonFile, parameters);
    }

    public AutomatedPythonJobBuilder createJob(final PythonScript pythonScript, final File pythonFile) {
        return new AutomatedPythonJobBuilder(this.getJobsClient(), pythonScript, pythonFile);
    }

    public AutomatedPythonJobBuilder createJob(final PythonScript pythonScript) {
        return new AutomatedPythonJobBuilder(this.getJobsClient(), pythonScript);
    }

    public AutomatedPythonJobBuilder createJob(final PythonScript pythonScript, final List<String> parameters) {
        return new AutomatedPythonJobBuilder(this.getJobsClient(), pythonScript, parameters);
    }

    public AutomatedSparkSubmitJobBuilder createJob(final List<String> parameters) {
        return new AutomatedSparkSubmitJobBuilder(this.getJobsClient(), parameters);
    }

    public void putDbfsFile(final File file, final String dbfsPath, final boolean overwrite) throws DbfsException {
        DbfsHelper.putFile(this.getDbfsClient(), file, dbfsPath, overwrite);
    }

    public void putDbfsFile(final File file, final String dbfsPath) throws DbfsException {
        DbfsHelper.putFile(this.getDbfsClient(), file, dbfsPath);
    }

    public byte[] getDbfsObject(final String dbfsPath) throws DbfsException {
        return DbfsHelper.getObject(this.getDbfsClient(), dbfsPath);
    }

    public DbfsFileInfo getDbfsObjectStatus(final String dbfsPath) throws HttpException {
        return new DbfsFileInfo(this.getDbfsClient().getStatus(dbfsPath));
    }

    public void deleteDbfsObject(final String dbfsPath, final boolean recursive) throws HttpException {
        final DbfsDeleteRequestDTO dbfsDeleteRequestDTO = new DbfsDeleteRequestDTO();
        dbfsDeleteRequestDTO.Path = dbfsPath;
        dbfsDeleteRequestDTO.Recursive = recursive;
        this.getDbfsClient().delete(dbfsDeleteRequestDTO);
    }

    public void moveDbfsObject(final String fromPath, final String toPath) throws HttpException {
        this.getDbfsClient().move(fromPath, toPath);
    }

    public void mkdirsDbfs(final String path) throws HttpException {
        this.getDbfsClient().mkdirs(path);
    }

    public ArrayList<DbfsFileInfo> listDbfs(final String path) throws HttpException {
        final DbfsListResponseDTO dbfsListResponseDTO = this.getDbfsClient().list(path);
        final ArrayList<DbfsFileInfo> fileList = new ArrayList<DbfsFileInfo>();
        for (final FileInfoDTO fileInfo : dbfsListResponseDTO.Files) {
            fileList.add(new DbfsFileInfo(fileInfo));
        }
        return fileList;
    }

    public JarLibrary getJarLibrary(final URI uri) throws LibraryConfigException {
        return new JarLibrary(this.getLibrariesClient(), uri);
    }

    public EggLibrary getEggLibrary(final URI uri) throws LibraryConfigException {
        return new EggLibrary(this.getLibrariesClient(), uri);
    }

    public MavenLibrary getMavenLibrary(final String coordinates) throws LibraryConfigException {
        return new MavenLibrary(this.getLibrariesClient(), coordinates);
    }

    public MavenLibrary getMavenLibrary(final String coordinates, final String repo) throws LibraryConfigException {
        return new MavenLibrary(this.getLibrariesClient(), coordinates, repo);
    }

    public MavenLibrary getMavenLibrary(final String coordinates, final String repo, final String[] exclusions) throws LibraryConfigException {
        return new MavenLibrary(this.getLibrariesClient(), coordinates, repo, exclusions);
    }

    public MavenLibrary getMavenLibrary(final String coordinates, final String[] exclusions) throws LibraryConfigException {
        return new MavenLibrary(this.getLibrariesClient(), coordinates, exclusions);
    }

    public PyPiLibrary getPyPiLibrary(final String packageName) throws LibraryConfigException {
        return new PyPiLibrary(this.getLibrariesClient(), packageName);
    }

    public PyPiLibrary getPyPiLibrary(final String packageName, final String repo) throws LibraryConfigException {
        return new PyPiLibrary(this.getLibrariesClient(), packageName, repo);
    }

    public CranLibrary getCranLibrary(final String packageName) throws LibraryConfigException {
        return new CranLibrary(this.getLibrariesClient(), packageName);
    }

    public CranLibrary getCranLibrary(final String packageName, final String repo) throws LibraryConfigException {
        return new CranLibrary(this.getLibrariesClient(), packageName, repo);
    }

    public PythonScript getPythonScript(final URI uri) throws ResourceConfigException {
        return new PythonScript(this, uri);
    }

    public void mkdirsWorkspace(final String workspacePath) throws WorkspaceConfigException {
        try {
            final WorkspaceMkdirsRequestDTO workspaceMkdirsRequestDTO = new WorkspaceMkdirsRequestDTO();
            workspaceMkdirsRequestDTO.Path = workspacePath;
            this.getWorkspaceClient().mkdirs(workspaceMkdirsRequestDTO);
        }
        catch (HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public void deleteWorkspaceObject(final String workspacePath, final boolean recursive) throws WorkspaceConfigException {
        try {
            final WorkspaceDeleteRequestDTO workspaceDeleteRequestDTO = new WorkspaceDeleteRequestDTO();
            workspaceDeleteRequestDTO.Path = workspacePath;
            workspaceDeleteRequestDTO.Recursive = recursive;
            this.getWorkspaceClient().delete(workspaceDeleteRequestDTO);
        }
        catch (HttpException e) {
            throw new WorkspaceConfigException(e);
        }
    }

    public Notebook getNotebook(final String workspacePath) throws WorkspaceConfigException {
        return this.getWorkspaceHelper().getNotebook(workspacePath);
    }

    public ScalaNotebookBuilder createScalaNotebook() throws WorkspaceConfigException {
        return new ScalaNotebookBuilder(this.getWorkspaceClient());
    }

    public ScalaNotebookBuilder createScalaNotebook(final File file) throws WorkspaceConfigException {
        return new ScalaNotebookBuilder(this.getWorkspaceClient(), file);
    }

    public CreateInstancePoolBuilder createInstancePool() {
        return new CreateInstancePoolBuilder(this.getInstancePoolsClient());
    }

    public InstancePool getInstancePool(final String instancePoolId) throws InstancePoolConfigException {
        try {
            return new InstancePool(this.getInstancePoolsClient(), this.getInstancePoolsClient().getInstancePool(instancePoolId));
        }
        catch (HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

    public InstancePool getFirstInstancePoolByName(final String instancePoolName) throws InstancePoolConfigException {
        try {
            final InstancePoolListResponseDTO instancePoolListDTO = this.getInstancePoolsClient().listInstancePools();
            if (instancePoolListDTO != null) {
                for (final InstancePoolGetResponseDTO pool : instancePoolListDTO.InstancePools) {
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
        return this.Endpoint;
    }

    public String getToken() {
        return this._databricksClientConfig.getWorkspaceToken();
    }

    public String getUserAgent() {
        return this._databricksClientConfig.getUserAgent();
    }
}