package com.hatcherdev.healthcheck;

import com.datastax.driver.core.*;
import com.datastax.driver.dse.*;

import java.util.List;
import java.util.Set;

public class HealthCheckService {

    private DseCluster cluster;
    private String dataCenter;

    public HealthCheckService(DseCluster injectedCluster, String injectedDataCenter) {
        cluster = injectedCluster;
        dataCenter = injectedDataCenter;
    }

    private KeyspaceMetadata getLocalKeyspaceForHealthCheck() {

        List<KeyspaceMetadata> allKeyspaceMetadata = cluster.getMetadata().getKeyspaces();

        for(KeyspaceMetadata km : allKeyspaceMetadata) {
            //filter out keyspaces that aren't NetworkTopologyStrategy
            if (km.getReplication().containsKey("class")) {
                String replicationClass = km.getReplication().get("class");
                if (!replicationClass.equalsIgnoreCase("org.apache.cassandra.locator.NetworkTopologyStrategy")) {
                    continue;
                }
            }

            //filter out keyspaces that aren't replicated to the local data center
            if (km.getReplication().containsKey(dataCenter)) { //TODO: is this case sensitive
                return km;
            }
        }

        return null;

    }

    public Boolean health() {
        Set<TokenRange> tokenRangeSet = cluster.getMetadata().getTokenRanges();
        KeyspaceMetadata km = getLocalKeyspaceForHealthCheck();

        if (km == null) {
            return false;
            //maybe throw an exception here instead?
        }

        int numberOfReplicas = 0;
        if (km.getReplication().containsKey(dataCenter)) {
            try {
                numberOfReplicas = Integer.parseInt(km.getReplication().get(dataCenter));
            }
            catch(Exception ex) {
                return false;
                //maybe throw an exception here instead?
            }
        }

        final int quorum = (numberOfReplicas + 1) / 2;
        Boolean health = tokenRangeSet
                .stream()
                .allMatch( tr ->
                        cluster.getMetadata().getReplicas(km.getName(), tr)
                                .stream()
                                .filter(h -> h.getDatacenter().equals(dataCenter))
                                .filter(Host::isUp)
                                .count() >= quorum
                );

        return health;

    }
}
