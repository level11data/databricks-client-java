package com.level11data.databricks.job.run;


import java.util.List;

public interface StandardJobRun extends JobRun {

    List<String> getBaseParameters();

    List<String> getOverridingParameters();
}
