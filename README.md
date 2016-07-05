# Galaxy Java SDK User Guide

Including SDKs of XiaoMi SDS, EMQ, EMR, and Talos.

## INSTALLATION
* If using maven to management, you can just depend latest jars from maven central repository.

 - Checkout to the newest release and use its version in dependency.

 - Use corresponding SDK artifactId.

 - For example, to use EMQ,
```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-emq-client</artifactId>
      <version>1.2.7</version>
    </dependency>
```
* Download source codes from github and compile it your self.
Github link: [https://github.com/XiaoMi/galaxy-sdk-java.git]()
For example, in maven using ```mvn clean install```


# NOTICE

* ```HttpClient``` of our SDKs support multi-thread, if you need greater concurrency,
you can create your own ```HttpClient``` and pass it to the constructor of class `ClientFactory`.
(References and examples in ```galaxy-client-java```, ```sds-android-examples```)：
```ClientFactory(Credential credential, HttpClient httpClient)```

* When compiling ```sds-android-examples```, you must install `Android SDK` and set
environment variable `ANDROID_HOME` to the install path of `Android SDK` before.
Our examples used `Android SDK API Level 19`.
