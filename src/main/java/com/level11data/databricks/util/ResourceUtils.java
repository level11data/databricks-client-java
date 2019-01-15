package com.level11data.databricks.util;

import com.level11data.databricks.session.WorkspaceSession;
import com.level11data.databricks.dbfs.DbfsException;
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
            throw new ResourceConfigException("Unsupported URI scheme: None");
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
            throw new ResourceConfigException("Unsupported URI scheme: " + scheme);
        }
    }


    public static void uploadFile(WorkspaceSession session, File file, URI destination) throws ResourceConfigException {
        validate(destination);

        try{
            //TODO add support for s3, s3a, s3n, azure
            if(destination.getScheme().equals("dbfs")) {
                session.putDbfsFile(file, destination.toString());
            } else {
                throw new ResourceConfigException(destination.getScheme() + " is not a supported scheme for upload");
            }
        } catch (DbfsException e) {
            throw new ResourceConfigException(e);
        }
    }

    public static String encodeToBase64(byte[] byteBuffer) throws IOException {
        Base64.Encoder encoder = Base64.getEncoder();
        byte[] fileBase64Bytes = encoder.encode(byteBuffer);
        return new String(encoder.encode(byteBuffer), StandardCharsets.UTF_8);
    }

    public static String encodeToBase64(File file) throws IOException {
        try(FileInputStream fileInputStreamReader = new FileInputStream(file)) {
            byte[] fileBytes = new byte[(int)file.length()];
            fileInputStreamReader.read(fileBytes);
            return encodeToBase64(fileBytes);
        }
    }

    public static byte[] decodeFromBase64(String encodedBase64) throws IOException {
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
            Base64.Decoder decoder = Base64.getDecoder();
            return decoder.decode(encodedBase64);
        }
    }

    public static String getMD5(File file) throws IOException {
        try(FileInputStream inputStream = new FileInputStream(file)) {
            String md5 = getMD5(inputStream);
            inputStream.close();
            return md5;
        }
    }

    private static String getMD5(InputStream is) throws IOException {
        // md5Hex converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
        // The returned array will be double the length of the passed array, as it takes two characters to represent any given byte.
        return DigestUtils.md5Hex(IOUtils.toByteArray(is));
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
        File outputFile = new File(pathname);

        try(FileOutputStream out = new FileOutputStream(outputFile)) {
            out.write(bytes);
            return outputFile;
        } catch(IOException e) {
            throw new ResourceConfigException(e);
        }
    }

    public static File getResourceByName(String resourceName) throws ResourceConfigException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(resourceName).getFile();
        if(localPath != null) {
            return new File(localPath);
        } else {
            throw new ResourceConfigException("Resource Not Found: "+ resourceName);
        }
    }

}
