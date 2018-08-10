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
                               String dbfsPath) throws FileNotFoundException, IOException, HttpException  {
        putFile(client, file, dbfsPath, false);
    }

    public static void putFile(DbfsClient client,
                               File file,
                               String dbfsPath,
                               boolean overwrite) throws IOException, HttpException {

        FileInputStream fileInputStreamReader = new FileInputStream(file);
        try {
            byte[] fileBytes = new byte[(int)file.length()];
            fileInputStreamReader.read(fileBytes);

            //encode bytes to Base64
            Base64.Encoder encoder = Base64.getEncoder();
            byte[] fileBase64Bytes = encoder.encode(fileBytes);

            long bytesLeftToSend = fileBase64Bytes.length;

            if(bytesLeftToSend < MAX_BLOCK_SIZE) {
                PutRequestDTO putRequestDTO = new PutRequestDTO();
                putRequestDTO.Path = dbfsPath;
                putRequestDTO.Contents = ResourceUtils.encodeToBase64(file);
                putRequestDTO.Overwrite = overwrite;
                client.put(putRequestDTO);
            } else {
                //open handler to DBFS
                CreateRequestDTO createRequestDTO = new CreateRequestDTO();
                createRequestDTO.Path = dbfsPath;
                createRequestDTO.Overwrite = overwrite;
                long dbfsHandle = client.create(createRequestDTO);

                //reset the reader to begin reading from the start again
                fileInputStreamReader = new FileInputStream(file);

                while(bytesLeftToSend > 0) {
                    int numBytesToSend = (int) bytesLeftToSend > MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : (int) bytesLeftToSend;
                    byte[] bytesToSend = new byte[(int)numBytesToSend];

                    //read the next batch of bytes that fits into the byte array
                    fileInputStreamReader.read(bytesToSend);

                    //TODO add retry logic; inside or outside the client?

                    //add block to DBFS
                    AddBlockRequestDTO addBlockRequestDTO = new AddBlockRequestDTO();
                    addBlockRequestDTO.Handle = dbfsHandle;
                    addBlockRequestDTO.Data = encoder.encodeToString(bytesToSend);
                    client.addBlock(addBlockRequestDTO);

                    bytesLeftToSend = bytesLeftToSend - numBytesToSend;
                }
                //close handler to DBFS
                CloseRequestDTO closeRequestDTO = new CloseRequestDTO();
                closeRequestDTO.Handle = dbfsHandle;
                client.close(closeRequestDTO);
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
