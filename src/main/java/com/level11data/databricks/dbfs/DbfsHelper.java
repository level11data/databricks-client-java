package com.level11data.databricks.dbfs;

import com.level11data.databricks.client.DbfsClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.dbfs.FileInfoDTO;
import com.level11data.databricks.client.entities.dbfs.ReadResponseDTO;

import java.io.*;
import java.util.Base64;

public class DbfsHelper {
    private static final int MAX_BLOCK_SIZE = 1048576; //1 MB


    public static void putFile(DbfsClient client,
                               File file,
                               String path) throws FileNotFoundException, IOException, HttpException  {
        putFile(client, file, path, false);
    }

    public static void putFile(DbfsClient client,
                               File file,
                               String path,
                               boolean overwrite) throws FileNotFoundException, IOException, HttpException {
        long bytesLeftToRead = file.length();
        //TODO add call to .put() if less than MAX_BLOCK_SIZE

        FileInputStream fileInputStreamReader = new FileInputStream(file);

        try {
            //open handler to DBFS
            long dbfsHandle = client.create(path, overwrite);

            while(bytesLeftToRead > 0) {
                int numBytesToSend = (int) bytesLeftToRead > MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : (int) bytesLeftToRead;
                byte[] bytesToSend = new byte[(int)numBytesToSend];

                //read the next batch of bytes that fits into the byte array
                int numBytesRead = fileInputStreamReader.read(bytesToSend);

                //encode bytes to Base64
                Base64.Encoder encoder = Base64.getEncoder();

                //TODO add retry logic; inside or outside the client?
                //add block to DBFS
                client.addBlock(dbfsHandle, encoder.encode(bytesToSend));

                bytesLeftToRead = bytesLeftToRead - numBytesRead;
            }
            //close handler to DBFS
            client.close(dbfsHandle);
        } catch (Throwable e) {
          throw e;
        } finally {
            fileInputStreamReader.close();
        }

    }

    public static byte[] getObject(DbfsClient client, String path) throws IOException, HttpException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            FileInfoDTO statusResponseDTO = client.getStatus(path);
            long bytesLeftToWrite = statusResponseDTO.FileSize;

            while(bytesLeftToWrite > 0) {
                ReadResponseDTO readResponseDTO = client.read(path, 0, MAX_BLOCK_SIZE);
                long numBytesToWrite = readResponseDTO.BytesRead;

                System.out.println("bytesLeftToWrite: "+bytesLeftToWrite + ", numBytesToWrite: "+numBytesToWrite);

                Base64.Decoder decoder = Base64.getDecoder();
                byte[] decodedBytes = decoder.decode(readResponseDTO.data);
                outputStream.write(decodedBytes);
                bytesLeftToWrite = bytesLeftToWrite - numBytesToWrite;
            }
            return outputStream.toByteArray();
        } catch(Throwable e) {
            throw e;
        } finally {
            outputStream.close();
        }
    }


}
