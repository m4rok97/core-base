package org.ignis.properties;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;

public final class ICrypto {

    private static final byte[] SALTED = "Salted__".getBytes();

    private ICrypto() {
    }

    private static Cipher newCipher(String secret, byte[] salt, int mode)
            throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, InvalidKeyException {
        var keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        var keySpec = new PBEKeySpec(secret.toCharArray(), salt, 10000, 48 * 8);

        var keyIv = keyFactory.generateSecret(keySpec).getEncoded();

        var key = new SecretKeySpec(Arrays.copyOf(keyIv, keyIv.length - 16), "AES");
        var iv = Arrays.copyOfRange(keyIv, keyIv.length - 16, keyIv.length);

        var cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(mode, key, new IvParameterSpec(iv));
        return cipher;
    }

    public static String encode(String value, String secret) {
        if (secret == null) {
            return value;
        }
        try {
            byte[] salt = (new SecureRandom()).generateSeed(8);
            var cipher = newCipher(secret, salt, Cipher.ENCRYPT_MODE);

            var baos = new ByteArrayOutputStream();
            var valueEncrypted = cipher.doFinal(value.getBytes(StandardCharsets.UTF_8));
            baos.write(SALTED);
            baos.write(salt);
            baos.write(valueEncrypted);

            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String decode(String value, String secret) {
        if (secret == null) {
            return value;
        }
        try {
            var rawValue = Base64.getDecoder().decode(value);
            if (Arrays.compare(rawValue, 0, SALTED.length, SALTED, 0, SALTED.length) != 0) {
                throw new RuntimeException("bad magic number");
            }

            var salt = Arrays.copyOfRange(rawValue, SALTED.length, SALTED.length + 8);
            var valueEncrypted = Arrays.copyOfRange(rawValue, SALTED.length + 8, rawValue.length);

            var cipher = newCipher(secret, salt, Cipher.DECRYPT_MODE);
            return new String(cipher.doFinal(valueEncrypted), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String openssl(String value, String secretFile, boolean decode) throws IOException {
        Process openssl = Runtime.getRuntime().exec(new String[]{
                "ignis-crypto", decode ? "decode" : "encode", secretFile
        });
        openssl.getOutputStream().write(value.getBytes());
        openssl.getOutputStream().close();
        BufferedReader input = new BufferedReader(new InputStreamReader(openssl.getInputStream()));
        String result = input.readLine().trim();
        input.close();
        return result;
    }

    public record IKeyPair(String privateKey, String publicKey) {
    }

    public static IKeyPair genKeyPair() {
        try {
            ByteArrayOutputStream privateKey = new ByteArrayOutputStream(2048);
            ByteArrayOutputStream publicKey = new ByteArrayOutputStream(2048);
            var kpair = com.jcraft.jsch.KeyPair.genKeyPair(new JSch(), com.jcraft.jsch.KeyPair.RSA, 2048);
            kpair.writePrivateKey(privateKey);
            kpair.writePublicKey(publicKey, "");
            return new IKeyPair(privateKey.toString(), publicKey.toString());
        } catch (JSchException e) {
            throw new RuntimeException(e);
        }
    }

    public static TrustManager selfSignedTrust() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }

}
