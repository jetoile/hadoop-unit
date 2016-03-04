package fr.jetoile.hadoopunit;

import java.io.IOException;
import java.net.Socket;


public class Utils {

    public static boolean available(String url, int port) {
        try (Socket ignored = new Socket(url, port)) {
            return false;
        } catch (IOException ignored) {
            return true;
        }
    }
}
