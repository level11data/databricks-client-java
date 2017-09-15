package com.level11data.databricks.entities.clusters;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeTypes {
    @JsonProperty("node_types")
    public List<NodeType> NodeTypes;

    @JsonProperty("default_node_type_id")
    public String DefaultNodeTypeId;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("NodeTypes : " + this.NodeTypes + '\n');
        stringBuilder.append("DefaultNodeTypeId : " + this.DefaultNodeTypeId + '\n');
        return stringBuilder.toString();
    }


}
