package com.xiaomi.infra.galaxy.sds.android.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CannedAcl;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.EntityGroupSpec;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.LocalSecondaryIndexSpec;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.SecondaryIndexConsistencyMode;
import com.xiaomi.infra.galaxy.sds.thrift.TableMetadata;
import com.xiaomi.infra.galaxy.sds.thrift.TableQuota;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableCreator {
  private static ClientFactory clientFactory;
  private static AdminService.Iface adminClient;
  private static String appId = ""; // Set your AppId
  private static String secretKeyId = ""; // Set your AppKey
  private static String secretKey = ""; // Set your AppSecret
  private static UserType userType = UserType.APP_SECRET;
  private static final Logger LOG = LoggerFactory.getLogger(TableAccessor.class);
  private String endpoint;
  private String tableName;

  public TableCreator(String tableName, String endpoint) {
    this.tableName = tableName;
    this.endpoint = endpoint;
  }

  private void init() {
    Credential credential = new Credential().setSecretKey(secretKey).setSecretKeyId(secretKeyId)
        .setType(userType);
    //Here use the default HttpClient, a more suitable HttpClient can be set here
    clientFactory = new ClientFactory().setCredential(credential);
    adminClient = clientFactory
        .newAdminClient(endpoint + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
  }

  private TableSpec tableSpec() {
    EntityGroupSpec entityGroupSpec = new EntityGroupSpec().setAttributes(Arrays.asList(
        new KeySpec[] { new KeySpec().setAttribute("userId") })); // This entity group is for access control
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("noteId") });
    Map<String, LocalSecondaryIndexSpec> secondaryIndexSpecMap = new HashMap<String, LocalSecondaryIndexSpec>();
    LocalSecondaryIndexSpec mtimeIndex = new LocalSecondaryIndexSpec();
    mtimeIndex.setIndexSchema(Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("mtime") }));
    mtimeIndex.setProjections(Arrays.asList("title", "noteId"));
    mtimeIndex.setConsistencyMode(SecondaryIndexConsistencyMode.EAGER);
    secondaryIndexSpecMap.put("mtime", mtimeIndex);
    LocalSecondaryIndexSpec catIndex = new LocalSecondaryIndexSpec();
    catIndex.setIndexSchema(Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("category") }));
    catIndex.setConsistencyMode(SecondaryIndexConsistencyMode.LAZY);
    secondaryIndexSpecMap.put("cat", catIndex);
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("userId", DataType.STRING);
    attributes.put("noteId", DataType.INT64);
    attributes.put("title", DataType.STRING);
    attributes.put("content", DataType.STRING);
    attributes.put("version", DataType.INT64);
    attributes.put("mtime", DataType.INT64);
    attributes.put("category", DataType.STRING);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setEntityGroup(entityGroupSpec)
        .setPrimaryIndex(primaryKey)
        .setSecondaryIndexes(secondaryIndexSpecMap)
        .setAttributes(attributes)
        .setTtl(-1);

    TableMetadata tableMetadata = new TableMetadata();
    Map<String, List<CannedAcl>> appGrant = new HashMap<String, List<CannedAcl>>();
    appGrant.put(appId, Arrays.asList(CannedAcl.APP_SECRET_READ, CannedAcl.APP_SECRET_WRITE,
        CannedAcl.APP_USER_ENTITY_GROUP_READ, CannedAcl.APP_USER_ENTITY_GROUP_WRITE));
    tableMetadata.setQuota(new TableQuota().setSize(100 * 1024 * 1024))
        .setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20))
        .setAppAcl(appGrant);

    return new TableSpec().setSchema(tableSchema).setMetadata(tableMetadata);
  }

  public void createTable() throws Exception {
    init();
    try {
      adminClient.dropTable(tableName);
    } catch (Exception se) {
      //It's ok
    }
    TableSpec tableSpec = tableSpec();
    adminClient.createTable(tableName, tableSpec);
    LOG.info("Create table successfully");
  }
}
