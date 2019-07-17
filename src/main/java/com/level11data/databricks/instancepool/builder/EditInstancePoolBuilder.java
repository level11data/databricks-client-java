package com.level11data.databricks.instancepool.builder;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.InstancePoolsClient;
import com.level11data.databricks.client.entities.instancepools.InstancePoolEditRequestDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolInfoDTO;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.InstancePoolConfigException;

public class EditInstancePoolBuilder extends AbstractInstancePoolBuilder {
    private final InstancePoolsClient _client;
    private final String _instancePoolId;
    private final String _nodeTypeId;

    public EditInstancePoolBuilder(InstancePoolsClient client, InstancePool instancePool) {
        super(client);
        _client = client;

        //required fields and cannot be modified
        _instancePoolId = instancePool.getId();
        _nodeTypeId = instancePool.getNodeType().InstanceTypeId;
    }

    private void validate() throws InstancePoolConfigException {
        if(_nodeTypeId == null ||  _nodeTypeId.isEmpty()) {
            throw new InstancePoolConfigException("InstancePool requires NodeType");
        }
    }

    public EditInstancePoolBuilder withName(String instancePoolName) {
        return (EditInstancePoolBuilder)super.withName(instancePoolName);
    }

    public EditInstancePoolBuilder withMinIdleInstances(int minIdleInstances) {
        return (EditInstancePoolBuilder) super.withMinIdleInstances(minIdleInstances);
    }

    public EditInstancePoolBuilder withMaxCapacity(int maxCapacity) {
        return (EditInstancePoolBuilder) super.withMaxCapacity(maxCapacity);
    }

    public EditInstancePoolBuilder withIdleInstanceAutoTerminationMinutes(int idleInstanceAutoTerminationMinutes) {
        return (EditInstancePoolBuilder) super.withIdleInstanceAutoTerminationMinutes(idleInstanceAutoTerminationMinutes);
    }

    public InstancePool modify() throws InstancePoolConfigException {
        validate();
        InstancePoolEditRequestDTO editRequestDTO = new InstancePoolEditRequestDTO();
        InstancePoolInfoDTO infoDTO = new InstancePoolInfoDTO();

        //required fields & cannot be modified
        editRequestDTO.InstancePoolId = _instancePoolId;
        editRequestDTO.NodeTypeId = _nodeTypeId;

        //apply modified fields
        editRequestDTO = super.applySettings(editRequestDTO);

        try{
            _client.editInstancePool(editRequestDTO);
        } catch(HttpException e) {
            throw new InstancePoolConfigException(e);
        }

        //instantiate new InstancePool with full settings
        return _client.Session.getInstancePool(_instancePoolId);
    }
}
