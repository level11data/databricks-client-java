package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.TerminationReasonDTO;
import java.util.Map;

public class TerminationReason {

    public final TerminationCode Code;
    public final Map<String, String> Parameters;

    public TerminationReason(TerminationReasonDTO terminationReasonDTO) {
        Code = TerminationCode.valueOf(terminationReasonDTO.TerminationCode);
        Parameters = terminationReasonDTO.Parameters;
    }

}
