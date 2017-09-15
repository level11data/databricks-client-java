package com.level11data.databricks.cluster;

import com.level11data.databricks.client.ClustersClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.entities.clusters.ClusterInfo;
import com.level11data.databricks.cluster.ClusterConfigException;

import java.util.Iterator;

public class ClusterIter implements Iterator<Cluster> {
    private ClustersClient _client;
    private ClusterInfo[] _clusterInfos;
    private int _clusterInfoIndex = 0;

    public ClusterIter(ClustersClient client, ClusterInfo[] clusterInfos) {
        _client = client;
        _clusterInfos = clusterInfos;
    }

    public boolean hasNext() {
        return _clusterInfoIndex < _clusterInfos.length;
    }

    public Cluster next() throws RuntimeException {
        try {
            Cluster cluster = new Cluster(_client, _clusterInfos[_clusterInfoIndex]);
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
            return new Cluster(_client, _clusterInfos[0]);
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
