## 打包作业
 pom.xml注意事项：
- storm server 已经有了storm-core这个jar，所以scope需要设置为provided：
```
    <dependency>
        <groupId>org.apache.storm</groupId>
        <artifactId>storm-core</artifactId>
        <version>0.9.1-incubating-mdh1.0-SNAPSHOT</version>
        <scope>provided</scope>
    </dependency>
```
- 提交的作业需要包含依赖，所以需要把dependency一起打包：
```
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>attached</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                    <archive>
                        <manifest>
                            <mainClass></mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
```
以示例代码为例[storm emq examples](https://github.com/XiaoMi/galaxy-sdk-java/tree/master/galaxy-storm-emq/examples)，打包过程如下：
```sh
cd examples
mvn clean package
```
打包会生成我们接下来需要提交的文件：examples/target/galaxy-storm-emq-example-1.3-SNAPSHOT-jar-with-dependencies.jar
## 下载客户端
- 下载客户端：http://lg-hadoop-build03.bj/share/infra/storm/storm-0.9.1-incubating-mdh1.0-SNAPSHOT.tar.gz
- 解压缩
```sh
tar -zxf storm-0.9.1-incubating-mdh1.0-SNAPSHOT.tar.gz
cd storm-0.9.1-incubating-mdh1.0-SNAPSHOT
```
## 提交作业
以示例代码examples为例：
```sh
./storm jar examples/target/galaxy-storm-emq-example-1.3-SNAPSHOT-jar-with-dependencies.jar  storm.emq.example.SimpleEMQTopology storm-topology.yaml
```
说明：
- storm jar是提交作业的命令，详细用法说明可以查看./storm help jar
```
Syntax: [storm jar topology-jar-path class ...]
    Runs the main method of class with the specified arguments. 
    The storm jars and configs in ~/.storm are put on the classpath. 
    The process is configured so that StormSubmitter 
    (http://nathanmarz.github.com/storm/doc/backtype/storm/StormSubmitter.html)
    will upload the jar at topology-jar-path when the topology is submitted.
```
- storm jar 主要接收两个参数，topology-jar-with-dependencies和main class；第三个参数是传给main class的，里面包括了作业名称以及nimbus ip等，具体请参考examples/src/main/resources/storm-topology.yaml