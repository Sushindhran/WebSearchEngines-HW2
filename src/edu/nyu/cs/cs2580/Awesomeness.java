package edu.nyu.cs.cs2580;

import java.io.*;

public class Awesomeness implements Serializable {
    private static final long serialVersionUID = 1077111905740085030L;
    public static void main(String[] args) throws IOException{
        String bytes = "Hello there is a stupid string to test this thing.";
        byte[] buffer = bytes.getBytes();
        FileOutputStream outputStream = null;


        try {
            outputStream = new FileOutputStream("results");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        // write() writes as many bytes from the buffer
        // as the length of the buffer.
        // You can also // use // write(buffer, offset, length)
        // if you want to write a specific number of
        // bytes, or only part of the buffer.
        try {
            outputStream.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
