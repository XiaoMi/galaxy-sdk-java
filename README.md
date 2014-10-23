结构化存储Java SDK使用介绍
========================
1. 编译安装SDK的jar到本地(或发布到Maven仓库)

```
mvn clean install
```

2. 如果项目使用Maven进行依赖管理，在项目的pom.xml文件中加入`galaxy-client-java`依赖：

```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-client-java</artifactId>
      <version>1.3-SNAPSHOT</version>
    </dependency>
```

或者直接把三个jar文件放到项目的classpath中。

SDS Java SDK User Guide
========================
1. Build the source code and install the jars into local maven repository (or deploy to a central Maven repository)

```
mvn clean install
```

2. Import the above jars into the project classpath or add the following dependency if your project is managed with Maven:

```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-client-java</artifactId>
      <version>1.3-SNAPSHOT</version>
    </dependency>
``` 

   
注意事项
========================

1.  本sdk提供的默认`HttpClient`可以支持多线程，但是对最大线程数进行限制， 用户如果需要`HttpClient`具有更大的并发数，
可自行定制`HttpClient`，通过`ClientFactory`的构造函数传入(可参考galaxy-client-java和sds-android-examples的示例代码)：

```
    ClientFactory(Credential credential, HttpClient httpClient)
```

2.  使用Maven对sds-android-examples编译时，需要安装Android SDK且设置环境变量ANDROID_HOME指向Android SDK的安装路径。
本示例使用Android SDK API Level 19。
