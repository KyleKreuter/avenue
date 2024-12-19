package de.kyle.avenue;

public class AvenueApplication {
    public static void main(String[] args) {
        SingleNodeServer singleNodeServer = new SingleNodeServer();
        singleNodeServer.start(8000);
    }
}
