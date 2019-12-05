package org.example.pds_expiration;

import org.apache.ignite.Ignite;

public class Server1 {

    public static void main(String[] args) {
        Ignite ignite = Cluster.ignite("Server1", false);
    }

}
