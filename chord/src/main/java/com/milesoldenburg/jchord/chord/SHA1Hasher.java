package com.milesoldenburg.jchord.chord;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class SHA1Hasher {

    private String address;
    private byte[] hashedBytes = new byte[8];

    public SHA1Hasher() {}

    public SHA1Hasher(String address) {
        this.address = address;
        this.hash();
    }

    public void hash() {
        MessageDigest md;

        try {
            // Create new SHA-1 digest
            md = MessageDigest.getInstance("SHA-1");

            // Hash address
            byte[] addressBytes = md.digest(this.address.getBytes());

            // Create 4-byte segments from 20-byte hash then XOR them together to get final 4-byte hash
            for (int i = 0; i < 4; i++) {
                this.hashedBytes[i + 4] = (byte) (addressBytes[i] ^ addressBytes[i + 4] ^ addressBytes[i + 8] ^ addressBytes[i + 12] ^ addressBytes[i + 16]);
            }
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Error: SHA-1 DNE");
            e.printStackTrace();
        }
    }

    public String getHex() {
        return DatatypeConverter.printHexBinary(Arrays.copyOfRange(this.hashedBytes, 4, 8));
    }

    public long getLong() {
        return java.nio.ByteBuffer.wrap(this.hashedBytes).getLong();
    }

    public void setAddress(String address) {
        this.address = address;
        this.hash();
    }

}
