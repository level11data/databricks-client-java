package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.ClusterInfoDTO;

import java.util.Iterator;

public class ClusterIter implements Iterator<Cluster> {
    private ClustersClient _client;
    private ClusterInfoDTO[] _clusterInfoDTOs;
    private int _clusterInfoIndex = 0;

    public ClusterIter(ClustersClient client, ClusterInfoDTO[] clusterInfoDTOs) {
        _client = client;
        _clusterInfoDTOs = clusterInfoDTOs;
    }

    public boolean hasNext() {
        return _clusterInfoIndex < _clusterInfoDTOs.length;
    }

    public Cluster next() throws RuntimeException {
        try {
            Cluster cluster = new Cluster(_client, _clusterInfoDTOs[_clusterInfoIndex]);
            _clusterInfoIndex++;
            return cluster;
        } catch (HttpException e) {
            throw(new RuntimeException(e));
        } catch (ClusterConfigException e) {
            throw(new RuntimeException(e));
        }
    }

    public Cluster first() throws RuntimeException {
        try {
            return new Cluster(_client, _clusterInfoDTOs[0]);
        } catch (HttpException e) {
            throw(new RuntimeException(e));
        } catch (ClusterConfigException e) {
            throw(new RuntimeException(e));
        }
    }

    public void remove() {
        //No Op
    }

}
