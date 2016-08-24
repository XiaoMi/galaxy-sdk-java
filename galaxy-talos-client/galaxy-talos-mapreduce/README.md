# dependency
  <pre><code>
  ```
  <groupId>com.xiaomi.infra.galaxy</groupId>
  <artifactId>galaxy-talos-mapreduce</artifactId>
  <version>{latest version}</version>
  ```
  </code></pre>

# Config
  <pre><code>
  **galaxy.talos.mapreduce.service.endpoint:**  The talos cluster which to use
  </pre></code>
  <pre><code>
  **galaxy.talos.mapreduce.topic.resource.name:**  The TopicTalosResrouceName to use, ***not the TopicName***;
  </pre></code>
  <pre><code>
  **galaxy.talos.maprduce.partition.offset:**  The partition offset info, it must in format "partition1:startMessageOffset:endMessageOffset,partition2:startMessageOffset:endMessageOffset", when will means we will consume partition1 and partition2 with offset [startMessageOffset, endMessageOffset]
  </pre></code>
  <pre><code>
  **galaxy.talos.mapreduce.secret.id:**  The SecretId for visit talos topic, for more information, please visit: http://docs.api.xiaomi.com/talos/api/auth.html"
  </pre></code>
  <pre><code>
  **galaxy.talos.mapreduce.secret.key:**  The SecretKey for visit talos topic, for more information, please visit: http://docs.api.xiaomi.com/talos/api/auth.html"
  </pre></code>
  <pre><code>
  **galaxy.talos.mapreduce.user.type:**  The UserType visit talos topic, it can only be **DEV_XIAOMI** or **APP_SECRET**, for more information, please visit: http://docs.api.xiaomi.com/talos/api/auth.html"
  </pre></code>
  <pre><code>
  **galaxy.talos.mapreduce.consume.data.max.retrys.before.fail.map.task:**  The max consume data retry time before fail the mapper task, default value is **5**
  </pre></code>
  
# Run example

## Prerequisite
  Download Hadoop package and unzip
  Set env $HADOOP_HOME to hadoop package root
  <pre><code>export HADOOP_HOME=package_path</code></pre>

## Build
  <pre><code>mvn package</code></pre>

## Run locally
  <pre><code>
  ```
  $HADOOP_HOME/bin/yarn jar 
  target/galaxy-talos-mapreduce-1.2-SNAPSHOT-jar-with-dependencies.jar
  com.xiaomi.infra.galaxy.talos.mapreduce.example.TalosMessageCount 
  -Dmapreduce.framework.name=local 
  -Dfs.defaultFS=file:/// 
  -Dgalaxy.talos.mapreduce.service.endpoint=xxx 
  -Dgalaxy.talos.mapreduce.topic.resource.name=xxx
  -Dgalaxy.talos.maprduce.partition.offset=xxx
  -Dgalaxy.talos.mapreduce.secret.id=xxx
  -Dgalaxy.talos.mapreduce.secret.key=xxx
  -Dgalaxy.talos.mapreduce.user.type=xxx
  [-Dgalaxy.talos.mapreduce.consume.data.max.retrys.before.fail.map.task=xxx]
  $your_local_output_path
  ```
  </code></pre>

## Run on cluster
  <pre><code>
  ```
  $HADOOP_HOME/bin/yarn jar 
  target/galaxy-talos-mapreduce-1.2-SNAPSHOT-jar-with-dependencies.jar
  com.xiaomi.infra.galaxy.talos.mapreduce.example.TalosMessageCount 
  -Dgalaxy.talos.mapreduce.service.endpoint=xxx 
  -Dgalaxy.talos.mapreduce.topic.resource.name=xxx
  -Dgalaxy.talos.maprduce.partition.offset=xxx
  -Dgalaxy.talos.mapreduce.secret.id=xxx
  -Dgalaxy.talos.mapreduce.secret.key=xxx
  -Dgalaxy.talos.mapreduce.user.type=xxx
  [-Dgalaxy.talos.mapreduce.consume.data.max.retrys.before.fail.map.task=xxx]
  $hdfs_cluster_user_path
  ```
  </code></pre>

  Using '-conf' to run job on cluster

  <pre><code>
    ```
    $HADOOP_HOME/bin/yarn jar
    target/galaxy-talos-mapreduce-1.2-SNAPSHOT-jar-with-dependencies.jar
    com.xiaomi.infra.galaxy.talos.mapreduce.example.TalosMessageCount
    -conf $your_talos-mr-site-xml_path
    $hdfs_cluster_user_path
    ```
  </code></pre>