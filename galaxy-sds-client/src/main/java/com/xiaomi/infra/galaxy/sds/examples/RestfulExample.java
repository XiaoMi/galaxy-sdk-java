package com.xiaomi.infra.galaxy.sds.examples;

import com.google.common.collect.LinkedListMultimap;
import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import com.xiaomi.infra.galaxy.client.authentication.HttpMethod;
import com.xiaomi.infra.galaxy.client.authentication.HttpKeys;
import com.xiaomi.infra.galaxy.client.authentication.signature.SignAlgorithm;
import com.xiaomi.infra.galaxy.client.authentication.signature.Signer;


import org.apache.http.client.methods.HttpPut;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;

public class RestfulExample {
    private static AdminService.Iface adminClient;
    private static TableService.Iface tableClient;
    private static String accountKey = "1-100"; // Your AccountKey
    private static String accountSecret = "100"; // Your AccountSecret
    private static String endpoint = "http://localhost:9086";
    private static boolean isInit = false;
    private static String tableName = "restful-example-test";


    private static Credential getCredential(String secretKeyId, String secretKey, UserType userType) {
        return new Credential().setSecretKeyId(secretKeyId).setSecretKey(secretKey)
                .setType(userType);
    }

    public static AdminService.Iface createAdminClient(String host) {
        Credential credential = getCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
        ClientFactory clientFactory = new ClientFactory().setCredential(credential);
        return clientFactory.newAdminClient(host + CommonConstants.ADMIN_SERVICE_PATH,
                50000, 3000);
    }

    public static TableService.Iface createTableClient(String host) {
        Credential credential = getCredential(accountKey, accountSecret, UserType.APP_SECRET);
        // based on JSON transport protocol
        // clientFactory = new ClientFactory().setCredential(credential).setProtocol(ThriftProtocol.TJSON);

        // based on Compact Binary transport protocol
        // clientFactory = new ClientFactory().setCredential(credential).setProtocol(ThriftProtocol.TCOMPACT);

        // based on default Binary transport protocol
        ClientFactory clientFactory = new ClientFactory().setCredential(credential);
        return clientFactory.newTableClient(host + CommonConstants.TABLE_SERVICE_PATH,
                10000, 3000, true, 5);
    }

    public static Map<String, List<CannedAcl>> cannedAclGrant(String appId, CannedAcl... cannedAcls) {
        Map<String, List<CannedAcl>> appGrant = new HashMap<String, List<CannedAcl>>();
        appGrant.put(appId, Arrays.asList(cannedAcls));
        return appGrant;
    }

    private static void init() {
        adminClient = createAdminClient(endpoint);
        tableClient = createTableClient(endpoint);
        isInit = true;
    }


    private static URI generatePresignedUri(String baseUri, Integer expiration,
                                            HttpMethod httpMethod, String accessId, String accessSecret,
                                            SignAlgorithm signAlgorithm) throws Exception {
        try {
            URI uri = new URI(baseUri);
            Long expirationTime = new Date().getTime() + expiration;
            URI encodedUri = new URI(baseUri + "?" + HttpKeys.GALAXY_ACCESS_KEY_ID + "=" + accessId
                    + "&" + HttpKeys.EXPIRES + "=" + expirationTime.toString());
            LinkedListMultimap<String, String> headers = null;
            String signature = Signer.signToBase64(httpMethod, encodedUri, headers,
                    accessSecret, signAlgorithm);
            return new URI(encodedUri.toString() + "&" + HttpKeys.SIGNATURE + "=" + new String(signature));
        } catch (URISyntaxException e) {
            System.out.print("Invalid URI syntax");
            throw new Exception("Invalid URI syntax", e);
        } catch (InvalidKeyException e) {
            System.out.print("Invalid secret key spec");
            throw new Exception("Invalid secret key spec", e);
        } catch (NoSuchAlgorithmException e) {
            System.out.print("Unsupported signature algorithm:" + signAlgorithm);
            throw new Exception("Unsupported signature algorithm:"
                    + signAlgorithm, e);
        }
    }

    private static void sendPut(URI uri, byte[] body) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPut httpPut = new HttpPut(uri);
        httpPut.setEntity(new ByteArrayEntity(body));
        try {
            HttpResponse httpResponse = httpClient.execute(httpPut);
            if (httpResponse.getStatusLine().getStatusCode() == 200) {
                System.out.println("Send Put Success");
                HttpEntity httpEntity = httpResponse.getEntity();
                String response = EntityUtils.toString(httpEntity);
                System.out.println(response);
            } else {
                System.out.println(httpResponse.getStatusLine().getStatusCode());
                System.out.println(httpResponse.getStatusLine().getReasonPhrase());
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static TableSpec tableSpec() {
        EntityGroupSpec entityGroupSpec = new EntityGroupSpec().setAttributes(Arrays.asList(
                new KeySpec[]{new KeySpec().setAttribute("date"),
                        new KeySpec().setAttribute("sn")})).setEnableHash(true);
        List<KeySpec> primaryKey = Arrays
                .asList(new KeySpec().setAttribute("recordid"));
        Map<String, DataType> attributes = new HashMap<String, DataType>();
        attributes.put("date", DataType.INT32);
        attributes.put("sn", DataType.STRING);
        attributes.put("recordid", DataType.STRING);
        attributes.put("recordvalue", DataType.BINARY);
        TableSchema tableSchema = new TableSchema();
        tableSchema.setEntityGroup(entityGroupSpec)
                .setPrimaryIndex(primaryKey)
                .setAttributes(attributes)
                .setTtl(-1);

        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata
                .setQuota(new TableQuota().setSize(100 * 1024 * 1024))
                .setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20));

        return new TableSpec().setSchema(tableSchema)
                .setMetadata(tableMetadata);
    }

    private static void printResult(Map<String, Datum> resultToPrint) {
        if (resultToPrint != null) {
            for (Map.Entry<String, Datum> e : resultToPrint.entrySet()) {
                if (e.getValue().getType() == DataType.BINARY) {
                    byte[] bytes = (byte[]) DatumUtil.fromDatum(e.getValue());
                    String newstring = new String(bytes);
                    System.out.println(
                            String.format("[%s] => %s", e.getKey(), newstring));

                } else {
                    System.out.println(
                            String.format("[%s] => %s", e.getKey(), DatumUtil.fromDatum(e.getValue()).toString()));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        init();
        TableSpec tableSpec = tableSpec();
        try {
            adminClient.createTable(tableName, tableSpec);
            tableSpec = adminClient.describeTable(tableName);
            System.out.println("table " + tableName + ":");
            System.out.println(tableSpec);
            URI putUrlBase = generatePresignedUri(endpoint + CommonConstants.RESTFUL_SERVICE_PATH, 6000000,
                    HttpMethod.PUT, accountKey, accountSecret, SignAlgorithm.HmacSHA1);

            for (Integer i = 0; i < 10; i++) {
                System.out.println("----------test-" + i + "----------");
                Integer data = 20170601 + i;
                String sn = "138710000123" + i;
                String recordid = "10;2" + i;
                URI putUrl = new URI(putUrlBase.toString() + "&tableName=" + tableName + "&" +
                        "date=" + data.toString() + "&sn=" + sn + "&recordid=" + recordid + "&recordvalue=");
                System.out.println("presigned url =" + putUrl);

                sendPut(putUrl, putUrl.toString().getBytes());
                GetRequest getRequest = new GetRequest();
                getRequest.setTableName(tableName);
                getRequest.putToKeys("date", DatumUtil.toDatum(data));
                getRequest.putToKeys("sn", DatumUtil.toDatum(sn));
                getRequest.putToKeys("recordid", DatumUtil.toDatum(recordid));
                GetResult getResult = tableClient.get(getRequest);
                printResult(getResult.getItem());
            }

        } finally {
            adminClient.disableTable(tableName);
            adminClient.dropTable(tableName);
        }
    }
}
