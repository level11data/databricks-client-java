package com.level11data.databricks.util;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.workspace.WorkspaceConfigException;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.codec.digest.DigestUtils;


public class ResourceUtils {

    public static void validate(URI uri) throws ResourceConfigException {
        String scheme = uri.getScheme();
        boolean isValid = false;

        if(scheme == null) {
            throw new ResourceConfigException("AbstractLibrary must be stored in dbfs or s3. Make sure the URI begins with 'dbfs:' or 's3:'");
        } else if(scheme.equals("dbfs")) {
            isValid = true;
        } else if(scheme.equals("s3")) {
            isValid = true;
        } else if(scheme.equals("s3a")) {
            isValid = true;
        } else if(scheme.equals("s3n")) {
            isValid = true;
        }

        if(!isValid) {
            throw new ResourceConfigException(scheme + " is NOT a valid URI scheme");
        }
    }


    public static void uploadFile(DatabricksSession session, File file, URI destination) throws HttpException, IOException, ResourceConfigException {
        validate(destination);

        //TODO add support for s3, s3a, s3n, azure
        if(destination.getScheme().equals("dbfs")) {
            session.putDbfsFile(file, destination.toString());
        } else {
            throw new ResourceConfigException(destination.getScheme() + " is not a supported scheme for upload");
        }
    }

    public static String encodeToBase64(byte[] byteBuffer) throws IOException {
        Base64.Encoder encoder = Base64.getEncoder();
        byte[] fileBase64Bytes = encoder.encode(byteBuffer);
        return new String(encoder.encode(byteBuffer), StandardCharsets.UTF_8);
    }

    public static String encodeToBase64(File file) throws IOException {
        FileInputStream fileInputStreamReader = null;
        try {
            byte[] fileBytes = new byte[(int)file.length()];
            fileInputStreamReader = new FileInputStream(file);
            fileInputStreamReader.read(fileBytes);
            return encodeToBase64(fileBytes);
        } finally {
            if (fileInputStreamReader != null) {
                fileInputStreamReader.close();
            }

        }
    }

    public static byte[] decodeFromBase64(String encodedBase64) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Base64.Decoder decoder = Base64.getDecoder();
        return decoder.decode(encodedBase64);
    }

    public static String getMD5(File file) throws IOException {
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            String md5 = getMD5(inputStream);
            inputStream.close();
            return md5;
        } finally {
            if(inputStream != null) {
                inputStream.close();
            }

        }

    }

    private static String getMD5(InputStream is) throws IOException {
        // md5Hex converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
        // The returned array will be double the length of the passed array, as it takes two characters to represent any given byte.
        String md5 = DigestUtils.md5Hex(IOUtils.toByteArray(is));

        return md5;
    }

    public static File writeTextFile(StringBuilder sb, String pathname) throws ResourceConfigException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(pathname))) {
            final int aLength = sb.length();
            final int aChunk = 1024;// 1 kb buffer to read data from
            final char[] aChars = new char[aChunk];

            for (int aPosStart = 0; aPosStart < aLength; aPosStart += aChunk) {
                final int aPosEnd = Math.min(aPosStart + aChunk, aLength);
                sb.getChars(aPosStart, aPosEnd, aChars, 0); // Create no new buffer
                bw.write(aChars, 0, aPosEnd - aPosStart);// This is faster than just copying one byte at the time
            }
            bw.flush();
        } catch (IOException e) {
            throw new ResourceConfigException(e);
        }
        return new File(pathname);
    }

    public static File writeBytesToFile(byte[] bytes, String pathname) throws ResourceConfigException {
        try {
            File outputFile = new File(pathname);
            FileOutputStream out = new FileOutputStream(outputFile);
            out.write(bytes);
            return outputFile;
        } catch(IOException e) {
            throw new ResourceConfigException(e);
        }
    }

}
