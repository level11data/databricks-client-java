package com.level11data.databricks.job;

import com.level11data.databricks.entities.jobs.RunStateDTO;

public class RunState {
    public final RunLifeCycleState LifeCycleState;
    public final RunResultState ResultState;
    public final String StateMessage;

    public RunState(RunStateDTO runStateDTO) {
        LifeCycleState = RunLifeCycleState.valueOf(runStateDTO.LifeCycleState);
        ResultState = RunResultState.valueOf(runStateDTO.ResultState);
        StateMessage = runStateDTO.StateMessage;
    }
}
