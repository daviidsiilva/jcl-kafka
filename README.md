# JCL
The [Java Cá&Lá](http://www.javacaela.org/) (or just JCL) is a middleware that integrates high performance computing (HPC) and Internet of things (IoT) in a unique API. For more details about JCL see [www.javacaela.org](http://www.javacaela.org/)

# JCL-Kafka
JCL event based

## Dependences
- Oracle Java 1.8
- Kafka 2.3

## Task lists
- [x] KafkaCluster on JCL_Host
- [x] JCL_User subscribe to JCL_Host
- [x] JCL_User perform instantiateGlobalVar(String, String) publishing to JCL_Host
- [x] JCL_User perform getValue(Object) locally
- [ ] JCL_User perform getValue(Object) without return some null values

## Useful Commands
- Initiate a new Zookeeper Server
```bash
.\bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

- Initiate a new Kafka Server
```bash
.\bin\windows\kafka-server-start.bat config\server.properties
```

- Initiate a Kafka Consumer from beginning streams
```bash
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic jcl-output --from-beginning
```
