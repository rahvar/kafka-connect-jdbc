package io.confluent.connect.jdbc.source;

/**
 * Created by shawnvarghese on 6/7/17.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.Mac;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;

public class DataTransform implements Transform {

    private static final Logger log = LoggerFactory.getLogger(DataTransform.class);
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

    public String transformString(String value, String transformer) {
        String hashtext = null;

        String strToAnon = value.replace("[","").replace("]","")
                .replace("\"","").replace("\\","").trim();

        if (value==null || value.trim().equals("")) {
            return value;
        }
        if (strToAnon.equals("")) {
            return "";
        }
        try {
            String message = strToAnon;
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
                String regex = "(?<=\""+path+"\":)(?:(?=.?\\[)(.*?\\])|((.*?)(?=[,}])))";
//                String regex1 = "(?<=\""+path+"\":)(.?\\[.*?\\])";
                Pattern p = Pattern.compile(regex);
                Matcher m = p.matcher(anonymizedString);
                StringBuffer sb = new StringBuffer(anonymizedString.length());
                while (m.find()) {
                    String match = m.group(0);
                    if (match.trim().startsWith("[")) {
                        m.appendReplacement(sb, transformStringArray(match,transformer));
                    }
                    else if (match.trim().startsWith("{") || match.trim().startsWith("[{")) {
                        continue;
                    }
                    else {
                        if (match.trim().startsWith("\\")) {
                            m.appendReplacement(sb, "\\\\\"" + transformString(match,transformer) + "\\\\\"");
                        }
                        else {
                            m.appendReplacement(sb, "\"" + transformString(match, transformer) + "\"");
                        }
                    }
                }
                m.appendTail(sb);
                anonymizedString = sb.toString();
            }
        }
        catch (Exception e) {
            log.trace("Anonymization unsuccessful. Please verify JSON path is of the format: column_name{key1&key2}");
            e.printStackTrace();
            anonymizedString = "{}";
        }
        if (anonymizedString == null)
            return null;
        return anonymizedString;
    }

    public String transformStringArray(String value,String transformer) {

        String strToAnon = value.replace("[","").replace("]","")
                        .replace("\"","").replace("\\","").trim();

        if (value == null || value.trim().equals("") || value.equals("[]") || strToAnon.equals("")) {
            return value;
        }

        String[] array = strToAnon.split(",");

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("[");
        for (int i = 0; i < array.length; i++) {
            strBuilder.append("\"" + transformString(array[i],transformer) + "\"");
            strBuilder.append(",");
        }
        strBuilder.replace(strBuilder.toString().lastIndexOf(","), strBuilder.toString().lastIndexOf(",") + 1, "]" );
        return strBuilder.toString();
    }

    public String transformTextArray(String value,String transformer) {

        if (value == null || value.trim().equals("") || value.equals("{}")) {
            return value;
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