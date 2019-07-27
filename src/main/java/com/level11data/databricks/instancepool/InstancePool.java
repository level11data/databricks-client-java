package com.level11data.databricks.instancepool;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.InstancePoolsClient;
import com.level11data.databricks.client.entities.instancepools.InstancePoolGetResponseDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolIdDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolInfoDTO;
import com.level11data.databricks.cluster.*;
import com.level11data.databricks.cluster.builder.AutomatedClusterBuilder;
import com.level11data.databricks.cluster.builder.InteractiveClusterBuilder;
import com.level11data.databricks.instancepool.builder.EditInstancePoolBuilder;

import java.util.ArrayList;
import java.util.Map;

public class InstancePool {

    private InstancePoolsClient _client;
    private final String _id;

    private String _name;
    private NodeType _nodeType;
    private Integer _minIdleInstances;
    private Integer _maxCapacity;
    private Integer _idleInstanceAutoTerminationMinutes;
    private Map<String,String> _customTags;
    private ArrayList<SparkVersion> _preloadedSparkVersions = new ArrayList<SparkVersion>();
    private ArrayList<String> _preloadedDockerImages = new ArrayList<String>();  //TODO create proper type for DockerImage
    private AwsAttributes _awsAttributes;
    private boolean _enableElasticDisk;
    private DiskSpec _diskSpec;

    public InstancePool(InstancePoolsClient client,
                        InstancePoolInfoDTO instancePoolInfoDTO,
                        String instancePoolId) throws InstancePoolConfigException {
        try{
            _client = client;
            _id = instancePoolId;
            _name = instancePoolInfoDTO.InstancePoolName;
            _nodeType = _client.Session.getNodeTypeById(instancePoolInfoDTO.NodeTypeId);
            _minIdleInstances = instancePoolInfoDTO.MinIdleInstances;
            _maxCapacity = instancePoolInfoDTO.MaxCapacity;
            _idleInstanceAutoTerminationMinutes = instancePoolInfoDTO.IdleInstanceAutoTerminationMinutes;
            _customTags = instancePoolInfoDTO.CustomTags;

            if(instancePoolInfoDTO.PreloadedSparkVersions != null) {
                for (String sparkVersion : instancePoolInfoDTO.PreloadedSparkVersions) {
                    _preloadedSparkVersions.add(_client.Session.getSparkVersionByKey(sparkVersion));
                }
            }

            if(instancePoolInfoDTO.PreloadedDockerImages != null) {
                for (String dockerImage : instancePoolInfoDTO.PreloadedDockerImages) {
                    _preloadedDockerImages.add(dockerImage);  //TODO create proper type for DockerImage
                }
            }

            _enableElasticDisk = instancePoolInfoDTO.EnableElasticDisk == null ? false : instancePoolInfoDTO.EnableElasticDisk;

            //databricks chooses some defaults if AwsAttributes are not set
            // therefore a get call must be made for the truth
            InstancePoolGetResponseDTO responseDTO = _client.getInstancePool(_id);

            if(responseDTO.AwsAttributes != null) {
                _awsAttributes = new AwsAttributes(responseDTO.AwsAttributes);
            }

            if(responseDTO.DiskSpec != null) {
                _diskSpec = new DiskSpec(responseDTO.DiskSpec);
            }
        } catch(ClusterConfigException e) {
            throw new InstancePoolConfigException(e);
        } catch(HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

    public InstancePool(InstancePoolsClient client,
                        InstancePoolGetResponseDTO instancePoolGetResponseDTO) throws InstancePoolConfigException {
        this(client, client.mapInstancePoolInfoDTO(instancePoolGetResponseDTO),
                instancePoolGetResponseDTO.InstancePoolId);
    }

    public String getId() {
        return _id;
    }

    public String getName() {
        return _name;
    }

    public NodeType getNodeType() {
        return _nodeType;
    }

    public int getMinIdleInstances() {
        return _minIdleInstances == null ? 0 : _minIdleInstances;
    }

    public Integer getMaxCapacity() {
        return _maxCapacity;
    }

    public Integer getIdleInstanceAutoTerminationMinutes() {
        return _idleInstanceAutoTerminationMinutes;
    }

    public Map<String,String> getCustomTags() {
        return _customTags;
    }

    public ArrayList<SparkVersion> getPreloadedSparkVersions() {
        return _preloadedSparkVersions;
    }

    public ArrayList<String> getPreloadedDockerImages() {
        return _preloadedDockerImages;
    }

    public AwsAttributes getAwsAttributes() {
        return _awsAttributes;
    }

    public DiskSpec getDiskSpec() {
        return _diskSpec;
    }

    public boolean isElasticDiskEnabled() {
        return _enableElasticDisk;
    }

    public InstancePoolStats getStats() throws InstancePoolConfigException {
        try {
            return new InstancePoolStats(_client.getInstancePool(_id).Stats);
        }catch(HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

    public AutomatedClusterBuilder createClusterSpec(Integer numWorkers) {
        return new AutomatedClusterBuilder(_client.Session.getClustersClient(), numWorkers)
                .withInstancePool(this);
    }

    public AutomatedClusterBuilder createClusterSpec( Integer minWorkers, Integer maxWorkers) {
        return new AutomatedClusterBuilder(_client.Session.getClustersClient(), minWorkers, maxWorkers)
                .withInstancePool(this);
    }

    public InteractiveClusterBuilder createCluster(String clusterName, Integer numWorkers) {
        return new InteractiveClusterBuilder(_client.Session.getClustersClient(), clusterName, numWorkers)
                .withInstancePool(this);
    }

    public InteractiveClusterBuilder createCluster(String clusterName, Integer minWorkers, Integer maxWorkers) {
        return new InteractiveClusterBuilder(_client.Session.getClustersClient(), clusterName, minWorkers, maxWorkers)
                .withInstancePool(this);
    }

    public boolean delete() throws InstancePoolConfigException {
        try {
            InstancePoolIdDTO idDTO = new InstancePoolIdDTO();
            idDTO.InstancePoolId = _id;
            return _client.deleteInstancePool(idDTO);
        } catch(HttpException e) {
            throw new InstancePoolConfigException(e);
        }
    }

    public EditInstancePoolBuilder edit() throws InstancePoolConfigException {
        return new EditInstancePoolBuilder(_client, this);
    }
}
