package org.example;

import org.apache.ignite.Ignite;

public class Server3 {

    public static void main(String[] args) {
        Ignite ignite = Cluster.ignite("Server3", false);
    }

}
