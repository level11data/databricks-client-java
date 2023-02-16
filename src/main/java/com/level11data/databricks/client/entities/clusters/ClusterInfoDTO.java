package com.level11data.databricks.client.entities.clusters;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.math.BigInteger;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.PropertyNamingStrategy.SnakeCaseStrategy;
import javax.ws.rs.Consumes;

@Consumes({ "application/json" })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClusterInfoDTO
{
    @JsonProperty("cluster_id")
    public String ClusterId;
    @JsonProperty("instance_pool_id")
    public String InstancePoolId;
    @JsonProperty("creator_user_name")
    public String CreatorUserName;
    @JsonProperty("driver")
    public SparkNodeDTO Driver;
    @JsonProperty("spark_context_id")
    public Long SparkContextId;
    @JsonProperty("executors")
    public SparkNodeDTO[] Executors;
    @JsonProperty("jdbc_port")
    public Integer JdbcPort;
    @JsonProperty("state")
    public String State;
    @JsonProperty("state_message")
    public String StateMessage;
    @JsonProperty("cluster_created_by")
    public String ClusterCreatedBy;
    @JsonProperty("cluster_source")
    public String ClusterSource;
    @JsonProperty("start_time")
    public Long StartTime;
    @JsonProperty("terminated_time")
    public BigInteger TerminatedTime;
    @JsonProperty("last_state_loss_time")
    public BigInteger LastStateLossTime;
    @JsonProperty("last_activity_time")
    public BigInteger LastActivityTime;
    @JsonProperty("cluster_memory_mb")
    public BigInteger ClusterMemoryMb;
    @JsonProperty("cluster_cores")
    public BigInteger ClusterCores;
    @JsonProperty("default_tags")
    public Map<String, String> DefaultTags;
    @JsonProperty("cluster_log_status")
    public LogSyncStatusDTO ClusterLogStatus;
    @JsonProperty("termination_reason")
    public TerminationReasonDTO TerminationReason;
    @JsonProperty("num_workers")
    public Integer NumWorkers;
    @JsonProperty("autoscale")
    public AutoScaleDTO AutoScale;
    @JsonProperty("cluster_name")
    public String ClusterName;
    @JsonProperty("spark_version")
    public String SparkVersionKey;
    @JsonProperty("node_type_id")
    public String NodeTypeId;
    @JsonProperty("driver_node_type_id")
    public String DriverNodeTypeId;
    @JsonProperty("aws_attributes")
    public AwsAttributesDTO AwsAttributes;
    @JsonProperty("autotermination_minutes")
    public Integer AutoTerminationMinutes;
    @JsonProperty("enable_elastic_disk")
    public Boolean EnableElasticDisk;
    @JsonProperty("spark_conf")
    public Map<String, String> SparkConf;
    @JsonProperty("ssh_public_keys")
    public String[] SshPublicKeys;
    @JsonProperty("custom_tags")
    public Map<String, String> CustomTags;
    @JsonProperty("cluster_log_conf")
    public ClusterLogConfDTO ClusterLogConf;
    @JsonProperty("spark_env_vars")
    public Map<String, String> SparkEnvironmentVariables;
    @JsonProperty("init_scripts")
    public List<InitScriptDTO> InitScripts;

    @Override
    public String toString() {
        final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            return ow.writeValueAsString((Object)this);
        }
        catch (JsonProcessingException e) {
            return "Could Not Marshal Object to JSON";
        }
    }
}
