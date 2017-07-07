package io.confluent.connect.jdbc.source;

/**
 * Created by shawnvarghese on 6/7/17.
 */

//import com.jayway.jsonpath.DocumentContext;
//import com.jayway.jsonpath.JsonPath;
//import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.Mac;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import org.apache.commons.codec.binary.Base64;

public class DataTransform implements Transform {

    private static KeyStore.SecretKeyEntry secret;

    DataTransform() {
        try {
            KeyStore ks = KeyStore.getInstance("JCEKS");
            char[] password = "RedL0ck!Anon".toCharArray();
            java.io.FileInputStream fis = null;

            fis = new java.io.FileInputStream("ANON_KEYSTORE_FILE");
            ks.load(fis, password);
            KeyStore.ProtectionParameter protParam =
                    new KeyStore.PasswordProtection(password);
            fis.close();

            secret = (KeyStore.SecretKeyEntry)ks.getEntry("anonymizeSKey",protParam);

        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnrecoverableEntryException e) {
            e.printStackTrace();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DataTransform.class);

    public String transformString(String value, String transformer) {
        String hashtext = null;
        if (value==null || value.trim().equals("")) {
            return value;
        }
        try {
            String message = value;
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            sha256_HMAC.init(secret.getSecretKey());
            String hash = Base64.encodeBase64String(sha256_HMAC.doFinal(message.getBytes()));

            hashtext = hash;

        } catch (NoSuchAlgorithmException e) {
            log.trace("Hashing failed");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }

        return hashtext;
    }

    public String transformJSON(String value, String transformer, String anonymizeKey, String fieldName) {
        String anonymizedString = null;
        try {
            String allPaths = anonymizeKey.substring((fieldName).length()+1,anonymizeKey.length())
                                                        .replace("{","").replace("}","");
            String[] pathList = allPaths.split("&");
            anonymizedString = value;
            for (String path:pathList) {
                String regex = "(?<="+path+"\".)(.*?)(?=,)";
                anonymizedString = anonymizedString.replaceAll(regex,"\""+transformString("$1",transformer)+"\"");
            }
        }
        catch (Exception e) {
            log.trace("Anonymization unsuccessful. Please verify JSON path is of the format: column_name#{path/to/key1&path/to/key2}");
            e.printStackTrace();
        }
        if (anonymizedString == null)
            return null;
        return anonymizedString;
    }

    public String transformStringArray(String value,String transformer) {
        if (value == null) {
            return null;
        }

        String[] array = value.replace("{","").replace("}","").split(",");

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("{");
        for (int i = 0; i < array.length; i++) {
            strBuilder.append(transformString(array[i],transformer));
            strBuilder.append(",");
        }
        strBuilder.replace(strBuilder.toString().lastIndexOf(","), strBuilder.toString().lastIndexOf(",") + 1, "}" );
        return strBuilder.toString();

    }
}