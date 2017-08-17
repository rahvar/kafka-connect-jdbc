package io.confluent.connect.jdbc.source;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashAnonymizer implements Transformer<Object>{

    private static final Logger log = LoggerFactory.getLogger(HashAnonymizer.class);
    private KeyStore.SecretKeyEntry secret;

    private HashAnonymizer () {
    }

    public static Transformer init(String keystorePass, String keystorePath) {
        HashAnonymizer anonymizer = null;
        try {
            KeyStore ks = KeyStore.getInstance("JCEKS");
            char[] password = keystorePass.toCharArray();
            java.io.FileInputStream fis = null;
            fis = new java.io.FileInputStream(keystorePath);
            ks.load(fis, password);
            KeyStore.ProtectionParameter protParam =
                    new KeyStore.PasswordProtection(password);
            fis.close();

            anonymizer = new HashAnonymizer();
            anonymizer.secret = (KeyStore.SecretKeyEntry)ks.getEntry("anonymizeSKey",protParam);

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
        return (Transformer) anonymizer;
    }

    public String transformString(String value) {
        String hashtext = null;

        if (value==null || value.trim().equals("")) {
            return value;
        }

        String strToAnon = value.replace("[","").replace("]","")
                .replace("\"","").replace("\\","").trim();

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
        catch (Exception e) {
            e.printStackTrace();
        }

        return hashtext;
    }

    public String transformJSON(String value, String anonymizeKey) {
        String anonymizedString = null;
        try {
            String allPaths = anonymizeKey.substring(anonymizeKey.indexOf("{")+1,anonymizeKey.length())
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
                        m.appendReplacement(sb, transformStringArray(match));
                    }
                    else if (match.trim().startsWith("{") || match.trim().startsWith("[{")) {
                        continue;
                    }
                    else {
                        if (match.trim().startsWith("\\")) {
                            m.appendReplacement(sb, "\\\\\"" + transformString(match) + "\\\\\"");
                        }
                        else {
                            m.appendReplacement(sb, "\"" + transformString(match) + "\"");
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

    public String transformStringArray(String value) {

        String strToAnon = value.replace("[","").replace("]","")
                .replace("\"","").replace("\\","").trim();

        if (value == null || value.trim().equals("") || value.equals("[]") || strToAnon.equals("")) {
            return value;
        }

        String[] array = strToAnon.split(",");

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("[");
        for (int i = 0; i < array.length; i++) {
            strBuilder.append("\"" + transformString(array[i]) + "\"");
            strBuilder.append(",");
        }
        strBuilder.replace(strBuilder.toString().lastIndexOf(","), strBuilder.toString().lastIndexOf(",") + 1, "]" );
        return strBuilder.toString();
    }

    public String transformTextArray(String value) {

        if (value == null || value.trim().equals("") || value.equals("{}")) {
            return value;
        }

        String[] array = value.replace("{","").replace("}","").split(",");

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("{");
        for (int i = 0; i < array.length; i++) {
            strBuilder.append(transformString(array[i]));
            strBuilder.append(",");
        }
        strBuilder.replace(strBuilder.toString().lastIndexOf(","), strBuilder.toString().lastIndexOf(",") + 1, "}" );
        return strBuilder.toString();
    }

    @Override
    public Object transform(int colType, Object value, String[] fieldArgs) {
        String colValue = null;
        switch (colType) {

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                colValue = transformString((String) value);
                break;
            }

            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                colValue = transformString((String) value);
                break;
            }

            case Types.ARRAY:
                colValue = transformTextArray((String) value);
                break;

            case Types.OTHER:
                colValue = transformJSON((String) value, fieldArgs[0]);
                break;

        }
        return colValue;
    }
}
