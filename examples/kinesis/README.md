## Specifying your Application's Apache Flink Version

When using Kinesis Data Analytics for Flink Runtime version 1.1.0, you specify the version of Apache Flink that your application uses when you compile your application. You provide the version of Apache Flink with the `-Dflink.version` parameter as follows:

```shell
mvn package -Dflink.version=1.8.2
```
