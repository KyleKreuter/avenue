package de.kyle.avenue.packet;

public interface Packet {
    byte[] getHeader();

    byte[] getBody();
}
