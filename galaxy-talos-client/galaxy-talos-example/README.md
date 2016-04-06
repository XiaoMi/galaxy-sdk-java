# Code lab for talos producer and consumer demo

## Prerequisite

* Apply your developer account in [Xiaomi developer platform](http://dev.xiaomi.com), get your $AppKey and $AppSecret

* Package your program with dependency talos client which refer to pom.xml in demo:

      ```
      <dependency>
        <groupId>com.xiaomi.infra.galaxy</groupId>
        <artifactId>galaxy-talos-client</artifactId>
        <version>1.3-SNAPSHOT</version>
      </dependency>
      ```

* Configure your log format and its level, make sure it in class path

## Run producer

    java -cp $your_class_path $your_system_properties com.xiaomi.infra.codelab.talos.TalosProducerDemo

## Run consumer

    java -cp $your_class_path $your_system_properties com.xiaomi.infra.codelab.talos.TalosConsumerDemo

## Talos Book

  [Talos Wiki](https://cnbj0.talos.api.xiaomi.com)
