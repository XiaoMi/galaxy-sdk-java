## 添加依赖
### clone repository
 ```sh
 git clone git@github.com:XiaoMi/galaxy-sdk-java.git
 ```
### build & install
```sh
cd galaxy-sdk-java
mvn clean install -DskipTests
```
### 添加依赖
```
<dependency>
   <groupId>com.xiaomi.infra.galaxy</groupId>
   <artifactId>galaxy-storm-emq</artifactId>
   <version>1.3-SNAPSHOT</version>
 </dependency>
 ```
## 初始化EMQSpout
### 初始化EMQConfig
 ```
 private EMQConfig getEMQConfig() {
     String queueName = "";//EMQ queue name
     String endpoint = "";//EMQ endpoint
     String tag = "";//EMQ tag, can be null. 如果tag为null，则采用EMQ默认tag。
     String keyId = "";//credential key id
     String key = "";//credential key
     Credential credential = new Credential().setSecretKeyId(keyId).setSecretKey(key).setType(UserType.APP_SECRET);
     return new EMQConfig(endpoint, queueName, tag, credential);
 }
 ```
### 初始化EMQSpout
```
EMQSpout spout = new EMQSpout(getEMQConfig());
```
### Storm Topology
根据需求配置storm topology即可。需要注意的是：storm topology的Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS 配置的值必须比 EMQ queue的 message InvisibilitySeconds要小，否则的话会提交topology失败。
比如设置的EMQ queue InvisibilitySeconds为30s，则可以设置storm topology config如下：
```
submitConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
```
## 自定义配置
以上步骤可以完成一个storm topology的配置，此外，还可以根据需求自定义一些配置。
### Scheme
EMQConfig默认的EMQScheme是DefaultEMQScheme，获取tuple数据方式：
```
String messageBody = tuple.getString(0);//get message body
Map<String, String> messageAttr = (Map<String,String>)tuple.getValue(1);//get message attributes
```
也可以自定义Scheme（参照DefaultEMQScheme），继承自EMQScheme即可，然后设置EMQConfig为自定义EMQScheme：
```
//create custom scheme
public class MyEMQScheme extends EMQScheme {

    @Override
	public List<Object> deserialize(final ReceiveMessageResponse response) {
	    ...
	}
    @Override
	public Fields getOutputFields() {
	    ...
	}
}
//use it
MyEMQScheme myEMQScheme = new MyEMQScheme(...);
EMQConfig emqConfig = new EMQConfig(...);
emqConfig.emqScheme = myEMQScheme;
```
### EMQCoordinator
对于EMQ的ReceiveMessageRequest，有许多参数可选，为了方便用户自定义，用户可自己实现EMQCoordinator，从而自定义ReceiveMessageRequest：
```
//create custom coordinator
public class MyEMQCoordinator implements EMQCoordinator {

    @Override
	public ReceiveMessageRequest newReceiveMessageRequest() {
	    return new ReceiveMessageRequest(...);//每次调用需要返回一个新的ReceiveMessageRequest
	}
}
//use it 
MyEMQCoordinator myEMQCoordinator = new MyEMQCoordinator(...);
EMQConfig emqConfig = new EMQConfig(...);
emqConfig.emqCoordinator = myEMQCoordinator;
```
## 示例代码
[storm emq examples](https://github.com/XiaoMi/galaxy-sdk-java/tree/master/galaxy-storm-emq/examples)

可以修改src/main/resource目录下的storm-topology.yaml来自定义配置。
