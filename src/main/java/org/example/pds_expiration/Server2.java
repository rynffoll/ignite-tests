package org.example.pds_expiration;

import org.apache.ignite.Ignite;

public class Server2 {

    public static void main(String[] args) {
        Ignite ignite = Cluster.ignite("Server2", false);
    }

}
