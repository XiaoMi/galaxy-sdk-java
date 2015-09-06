# 结构化存储Java SDK使用介绍

## 安装


1. 源码下载，然后编译安装SDK的jar到本地(或发布到Maven仓库)，
下载地址：[https://github.com/XiaoMi/galaxy-sdk-java.git]()

`mvn clean install`

2. 如果项目使用Maven进行依赖管理，可以直接从maven中央仓库下载1.2.1版本的sdk；

## 使用

1. 如果项目使用Maven管理，在项目的pom.xml文件中加入`galaxy-client-java`依赖，版本为自己编译的版本或maven中央仓库中的版本：

```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-client-java</artifactId>
      <version>1.2.1</version>
    </dependency>
```

2. 直接把三个jar文件放到项目的classpath中。

# SDS Java SDK User Guide

## Installing

1. Download source codes from github ,build and install the jars into local maven repository (or deploy to a central Maven repository).
github link: [https://github.com/XiaoMi/galaxy-sdk-java.git]()

`mvn clean install`

2. If use Maven to management, users can download the latest jars for maven central repository.

## Usage

1. Import the above jars into the project classpath or add the following dependency if your project is managed with Maven.

```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-client-java</artifactId>
      <version>1.2.1</version>
    </dependency>
```

2. Add the three jars to classpath.

   
# 注意事项

1.  本sdk提供的默认`HttpClient`可以支持多线程，但是对最大线程数进行限制， 用户如果需要`HttpClient`具有更大的并发数，
可自行定制`HttpClient`，通过`ClientFactory`的构造函数传入(可参考galaxy-client-java和sds-android-examples的示例代码)：

```
    ClientFactory(Credential credential, HttpClient httpClient)
```

2.  使用Maven对sds-android-examples编译时，需要安装Android SDK且设置环境变量ANDROID_HOME指向Android SDK的安装路径。
本示例使用Android SDK API Level 19。
