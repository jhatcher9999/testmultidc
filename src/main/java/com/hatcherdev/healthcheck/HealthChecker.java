package com.hatcherdev.healthcheck;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.*;

public class HealthChecker {


    public static void main(String[] args) throws InterruptedException {

        String host = "ip-10-200-181-3.datastax.lan";//args[0];
        String dataCenter = "dc1";

        LoadBalancingPolicy loadBalancingPolicy = new TokenAwarePolicy(
                new LatencyAwarePolicy.Builder(
                        DCAwareRoundRobinPolicy.builder()
                                .withLocalDc(dataCenter)
                                .build()
                )
                .build()
        );

        DseCluster cluster = DseCluster.builder()
                .addContactPoint(host)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build();

        HealthCheckService healthCheckService = new HealthCheckService(cluster, dataCenter);

        while(true) {

            Boolean health = healthCheckService.health();

            System.out.println("health: " + health);
            System.out.println("------------------------------------------------");
            System.out.println("");
            Thread.sleep(3000);
        }

    }


}
