package org.example.flink.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

/**
 * Minimum example of socket
 * @author Song Pan
 */
public class TcpSocketServer {

    /**
     * TCP(Transmission Control Protocol) connected, certain, ordered transport layer,in the form of BYTE STREAM
     * there will be three interactions[handshake] between server and client
     * very possible to transport mass of data in the channel between server-side and client-side, slightly sacrifice performance
     * ServerSocket : create socket at server-side and block to wait for client request
     * Socket : imply connector between server and client,
     *          there are one input stream and one output stream at both side of communicators
     *          use getInputStream() and getOutputStream() to send byte stream message to each other
     */
    public static void main(String[] args) throws IOException {
        System.out.println(">> Socket server running..");

        ServerSocket serverSocket = new ServerSocket(9999);

        while (true) {
            Socket socket = serverSocket.accept();
            System.out.println("> accept client: " + socket.getInetAddress().getHostName());
            CompletableFuture.runAsync(() -> {
                try {
                    while (true) {
                        System.out.println("> " + Thread.currentThread().getName());
                        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                        String str = br.readLine();

                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.write((str + System.lineSeparator()).getBytes());
                        System.out.println("> Send " + str);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

    }
}