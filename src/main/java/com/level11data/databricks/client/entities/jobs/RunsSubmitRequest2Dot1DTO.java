package com.level11data.databricks.client.entities.jobs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class RunsSubmitRequest2Dot1DTO {

  @JsonProperty("tasks")
  public JobTaskSettings2Dot1DTO[] Tasks;
  @JsonProperty("run_name")
  public String RunName;
  @JsonProperty("timeout_seconds")
  public Integer TimeoutSeconds;
  @JsonProperty("access_control_list")
  public AccessControlListDTO[] AccessControlList;

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
