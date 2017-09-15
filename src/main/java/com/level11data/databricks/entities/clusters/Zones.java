package com.level11data.databricks.entities.clusters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Zones {
    @JsonProperty("zones")
    public String[] Zones;

    @JsonProperty("default_zone")
    public String DefaultZone;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Zones : [" );

        int i = 0;
        for(String z: this.Zones){
            i++;
            if(i == this.Zones.length) {
                stringBuilder.append(z);
            } else {
                stringBuilder.append(z + ",");
            }
        }
        stringBuilder.append("]" + '\n');
        stringBuilder.append("DefaultZone : " + this.DefaultZone + '\n');
        return stringBuilder.toString();
    }
}
