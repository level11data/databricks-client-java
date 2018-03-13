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
                               String dbfsPath) throws FileNotFoundException, IOException, HttpException  {
        putFile(client, file, dbfsPath, false);
    }

    public static void putFile(DbfsClient client,
                               File file,
                               String dbfsPath,
                               boolean overwrite) throws FileNotFoundException, IOException, HttpException {

        FileInputStream fileInputStreamReader = new FileInputStream(file);
        try {
            byte[] fileBytes = new byte[(int)file.length()];
            fileInputStreamReader.read(fileBytes);
            System.out.println("Total Num of raw Bytes from File = " + file.length());

            //encode bytes to Base64
            Base64.Encoder encoder = Base64.getEncoder();
            byte[] fileBase64Bytes = encoder.encode(fileBytes);

            long bytesLeftToSend = fileBase64Bytes.length;

            System.out.println("Max Block Size                   ="+MAX_BLOCK_SIZE);
            System.out.println("Total Num of Base64 Bytes to Send=" + bytesLeftToSend);

            if(bytesLeftToSend < MAX_BLOCK_SIZE) {
                //TODO add call to .put() if less than MAX_BLOCK_SIZE
                System.out.println("TODO add call to .put() if less than MAX_BLOCK_SIZE");

                //open handler to DBFS
                long dbfsHandle = client.create(dbfsPath, overwrite);

                //TODO add retry logic; inside or outside the client?

                //add block to DBFS
                client.addBlock(dbfsHandle, fileBase64Bytes);

                //close handler to DBFS
                client.close(dbfsHandle);
            } else {
                //open handler to DBFS
                long dbfsHandle = client.create(dbfsPath, overwrite);

                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileBase64Bytes);

                while(bytesLeftToSend > 0) {
                    int numBytesToSend = (int) bytesLeftToSend > MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : (int) bytesLeftToSend;
                    byte[] bytesToSend = new byte[(int)numBytesToSend];

                    //read the next batch of bytes that fits into the byte array
                    int numBytesRead = byteArrayInputStream.read(bytesToSend);

                    //TODO add retry logic; inside or outside the client?

                    //add block to DBFS
                    System.out.println("BEFORE addBlock; base64Bytes     =" + numBytesToSend);
                    client.addBlock(dbfsHandle, bytesToSend);

                    //bytesLeftToSend = bytesLeftToSend - numBytesRead;
                    bytesLeftToSend = bytesLeftToSend - numBytesToSend;
                }
                //close handler to DBFS
                client.close(dbfsHandle);
            }
        } catch (Throwable e) {
          throw e;
        } finally {
            fileInputStreamReader.close();
        }

    }

    public static byte[] getObject(DbfsClient client, String dbfsPath) throws IOException, HttpException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            FileInfoDTO statusResponseDTO = client.getStatus(dbfsPath);
            long bytesLeftToRead = statusResponseDTO.FileSize;

            long offset = 0;

            while(bytesLeftToRead > 0) {
                ReadResponseDTO readResponseDTO = client.read(dbfsPath, offset, MAX_BLOCK_SIZE);
                long numBytesRead = readResponseDTO.BytesRead;

                //System.out.println("bytesLeftToRead: "+bytesLeftToRead + ", numBytesRead: "+numBytesRead);

                Base64.Decoder decoder = Base64.getDecoder();
                byte[] decodedBytes = decoder.decode(readResponseDTO.data);
                outputStream.write(decodedBytes);
                bytesLeftToRead = bytesLeftToRead - numBytesRead;
                offset = offset + numBytesRead;
            }
            return outputStream.toByteArray();
        } catch(Throwable e) {
            throw e;
        } finally {
            outputStream.close();
        }
    }


}
