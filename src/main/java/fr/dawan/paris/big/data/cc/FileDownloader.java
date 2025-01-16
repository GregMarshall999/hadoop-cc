package fr.dawan.paris.big.data.cc;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.net.URL;

public class FileDownloader {
    public static void downloadFile(String fileUrl, String destinationPath) throws Exception {
        try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrl).openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(destinationPath)) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        }
    }
}
