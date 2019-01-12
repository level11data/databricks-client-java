package com.level11data.databricks.dbfs;

import com.level11data.databricks.client.DbfsClient;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.dbfs.*;
import com.level11data.databricks.util.ResourceUtils;

import java.io.*;
import java.util.Base64;

public class DbfsHelper {
    private static final int MAX_BLOCK_SIZE = 1048576; //1 MB


    public static void putFile(DbfsClient client,
                               File file,
                               String dbfsPath) throws DbfsException  {
        putFile(client, file, dbfsPath, false);
    }

    private static byte[] getBase64Bytes(File file) throws IOException {
        try(BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            byte[] fileBytes = new byte[(int)file.length()];
            inputStream.read(fileBytes);

            //encode bytes to Base64
            Base64.Encoder encoder = Base64.getEncoder();
            return encoder.encode(fileBytes);
        }
    }

    private static String getBase64String(File file) throws IOException {
        try(BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            byte[] fileBytes = new byte[(int)file.length()];
            inputStream.read(fileBytes);
            return Base64.getEncoder().encodeToString(fileBytes);
        }
    }

    private static void putFileWithSingleCall(DbfsClient client,
                                              File file,
                                              String dbfsPath,
                                              boolean overwrite) throws IOException, HttpException {
        PutRequestDTO putRequestDTO = new PutRequestDTO();
        putRequestDTO.Path = dbfsPath;
        putRequestDTO.Contents =  getBase64String(file);
        putRequestDTO.Overwrite = overwrite;
        client.put(putRequestDTO);
    }

    private static long openDbfsHandle(DbfsClient client, String dbfsPath, boolean overwrite) throws HttpException {
        CreateRequestDTO createRequestDTO = new CreateRequestDTO();
        createRequestDTO.Path = dbfsPath;
        createRequestDTO.Overwrite = overwrite;
        return client.create(createRequestDTO);
    }

    private static void closeDbfsHandle(DbfsClient client, long dbfsHandle) throws HttpException {
        CloseRequestDTO closeRequestDTO = new CloseRequestDTO();
        closeRequestDTO.Handle = dbfsHandle;
        client.close(closeRequestDTO);
    }

    private static void addDbfsBlocks(DbfsClient client, long dbfsHandle, byte[] rawBytes) throws HttpException {
        AddBlockRequestDTO addBlockRequestDTO = new AddBlockRequestDTO();
        addBlockRequestDTO.Handle = dbfsHandle;
        addBlockRequestDTO.Data = Base64.getEncoder().encodeToString(rawBytes);
        client.addBlock(addBlockRequestDTO);
    }

    private static void putFileWithBlocks(DbfsClient client,
                                          File file,
                                          String dbfsPath,
                                          boolean overwrite) throws IOException, HttpException {
        //open handler to DBFS
        long dbfsHandle = openDbfsHandle(client, dbfsPath, overwrite);

        try(BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {

            while(inputStream.available() > 0) {
                int numBytesToSend = inputStream.available() > MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : inputStream.available();
                byte[] bytesToSend = new byte[numBytesToSend];

                //read the next batch of bytes that fits into the byte array
                inputStream.read(bytesToSend);

                //TODO add retry logic; inside or outside the client?

                //add block to DBFS
                addDbfsBlocks(client, dbfsHandle, bytesToSend);
            }
        }
        //close handler to DBFS
        closeDbfsHandle(client, dbfsHandle);
    }

    /**
     * Determine if file is under the maximum block size so that a single DBFS API call can be made
     * @param file
     * @return
     */
    private static boolean isFileSmall(File file) throws IOException {
        //file bytes is under max block size
        if(file.length() < MAX_BLOCK_SIZE) {
            //encode entire file to base64 and see if it is still under the max block size
            if(getBase64Bytes(file).length < MAX_BLOCK_SIZE) {
                return true;
            }
        }
        //otherwise
        return false;
    }

    public static void putFile(DbfsClient client,
                               File file,
                               String dbfsPath,
                               boolean overwrite) throws DbfsException {
        try {
            //TODO better way to determine if the encoded number of bytes is smaller than max block size?
            if(isFileSmall(file)) {
                //single dbfs put request
                putFileWithSingleCall(client, file, dbfsPath, overwrite);
            } else {
                //multiple dbfs requests
                putFileWithBlocks(client, file, dbfsPath, overwrite);
            }
        } catch (IOException e) {
            throw new DbfsException(e);
        } catch (HttpException e) {
            throw new DbfsException(e);
        }
    }

    public static byte[] getObject(DbfsClient client, String dbfsPath) throws DbfsException {
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            FileInfoDTO statusResponseDTO = client.getStatus(dbfsPath);
            long bytesLeftToRead = statusResponseDTO.FileSize;

            long offset = 0;

            while(bytesLeftToRead > 0) {
                ReadResponseDTO readResponseDTO = client.read(dbfsPath, offset, MAX_BLOCK_SIZE);
                long numBytesRead = readResponseDTO.BytesRead;

                Base64.Decoder decoder = Base64.getDecoder();
                byte[] decodedBytes = decoder.decode(readResponseDTO.data);
                outputStream.write(decodedBytes);
                bytesLeftToRead = bytesLeftToRead - numBytesRead;
                offset = offset + numBytesRead;
            }
            return outputStream.toByteArray();
        } catch(IOException e) {
            throw new DbfsException(e);
        } catch (HttpException e) {
            throw new DbfsException(e);
        }
    }


}
