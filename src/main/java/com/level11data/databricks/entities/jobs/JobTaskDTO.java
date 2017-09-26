package com.level11data.databricks.entities.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobTaskDTO {
    @JsonProperty("notebook_task")
    public NotebookTaskDTO NotebookTask;

    @JsonProperty("spark_jar_task")
    public SparkJarTaskDTO SparkJarTask;

    @JsonProperty("spark_python_task")
    public PythonTaskDTO SparkPythonTask;

    @JsonProperty("spark_submit_task")
    public SparkSubmitTaskDTO SparkSubmitTask;

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
