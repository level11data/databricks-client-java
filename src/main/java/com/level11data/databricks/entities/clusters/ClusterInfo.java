package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;
import java.util.Map;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterInfo {
    public enum ClusterState {
        PENDING, RUNNING, RESTARTING, RESIZING, TERMINATING, TERMINATED, ERROR, UNKNOWN
    }

    //These properties are only returned; they cannot be set
    @JsonProperty("cluster_id")
    public String ClusterId;

    @JsonProperty("creator_user_name")
    public String CreatorUserName;

    @JsonProperty("driver")
    public SparkNode Driver;

    //TODO This isn't in the documentation
    @JsonProperty("spark_context_id")
    public Float SparkContextId;

    //TODO This doesn't seem to ever be returned (via list); and why is this called "executors" if "workers" is used for sizing?
    @JsonProperty("executors")
    public SparkNode[] Executors;

    @JsonProperty("jdbc_port")
    public Integer JdbcPort;

    @JsonProperty("state")
    public ClusterState State;

    @JsonProperty("state_message")
    public String StateMessage;

    //This should probably be an enum if I knew the values (JOB_LAUNCHER, THIRD_PARTY)
    //TODO The list of possible values is not in the documentation
    @JsonProperty("cluster_created_by")
    public String ClusterCreatedBy;

    @JsonProperty("start_time")
    public BigInteger StartTime;

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
    public LogSyncStatus ClusterLogStatus;

    @JsonProperty("termination_reason")
    public TerminationReason TerminationReason;


    //These properites can be set
    @JsonProperty("num_workers")
    public Integer NumWorkers;

    @JsonProperty("autoscale")
    public AutoScale AutoScale;

    @JsonProperty("cluster_name")
    public String ClusterName;

    @JsonProperty("spark_version")
    public String SparkVersion;

    @JsonProperty("node_type_id")
    public String NodeTypeId;

    @JsonProperty("driver_node_type_id")
    public String DriverNodeTypeId;

    @JsonProperty("aws_attributes")
    public AwsAttributes AwsAttributes;

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
    public ClusterLogConf ClusterLogConf;

    @JsonProperty("spark_env_vars")
    public Map<String, String> SparkEnvironmentVariables;



    @Override
    public String toString() {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            return ow.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Could Not Marshal Object to JSON";
        }
    }

}
