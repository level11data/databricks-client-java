package com.level11data.databricks.instancepool.builder;

import com.level11data.databricks.client.InstancePoolsClient;
import com.level11data.databricks.client.entities.instancepools.InstancePoolEditRequestDTO;
import com.level11data.databricks.client.entities.instancepools.InstancePoolInfoDTO;
import com.level11data.databricks.instancepool.InstancePoolConfigException;

public abstract class AbstractInstancePoolBuilder {

    private final InstancePoolsClient _client;
    private String _name;
    private Integer _minIdleInstances;
    private Integer _maxCapacity;
    private Integer _idleInstanceAutoterminationMinutes;

    public AbstractInstancePoolBuilder(InstancePoolsClient client) {
        _client = client;
    }

    private void validate() throws InstancePoolConfigException {
        if(_name == null ||  _name.isEmpty()) {
            throw new InstancePoolConfigException("InstancePool requires Name");
        }
    }

    public AbstractInstancePoolBuilder withName(String instancePoolName) {
        _name = instancePoolName;
        return this;
    }

    public AbstractInstancePoolBuilder withMinIdleInstances(int minIdleInstances) {
        _minIdleInstances = minIdleInstances;
        return this;
    }

    public AbstractInstancePoolBuilder withMaxCapacity(int maxCapacity) {
        _maxCapacity = maxCapacity;
        return this;
    }

    public AbstractInstancePoolBuilder withIdleInstanceAutoTerminationMinutes(int idleInstanceAutoTerminationMinutes) {
        _idleInstanceAutoterminationMinutes = idleInstanceAutoTerminationMinutes;
        return this;
    }


    protected InstancePoolInfoDTO applySettings(InstancePoolInfoDTO instancePoolInfoDTO) throws InstancePoolConfigException {
        validate();
        instancePoolInfoDTO.InstancePoolName = _name;
        instancePoolInfoDTO.MinIdleInstances = _minIdleInstances;
        instancePoolInfoDTO.MaxCapacity = _maxCapacity;
        instancePoolInfoDTO.IdleInstanceAutoTerminationMinutes = _idleInstanceAutoterminationMinutes;
        return instancePoolInfoDTO;
    }

    protected InstancePoolEditRequestDTO applySettings(InstancePoolEditRequestDTO instancePoolEditRequestDTO)
            throws InstancePoolConfigException{
        validate();

        instancePoolEditRequestDTO.InstancePoolName = _name;
        instancePoolEditRequestDTO.MinIdleInstances = _minIdleInstances;
        instancePoolEditRequestDTO.MaxCapacity = _maxCapacity;
        instancePoolEditRequestDTO.IdleInstanceAutoTerminationMinutes = _idleInstanceAutoterminationMinutes;
        return instancePoolEditRequestDTO;
    }

}
