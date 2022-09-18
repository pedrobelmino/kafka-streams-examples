LAB para executar Kafka Streams de contagem de palavras
Código "retirado" e simplicado a partir de https://github.com/confluentinc/kafka-streams-examples
============================

**Pré-requisitos**
- Java 8
- Maven
- Docker
- Docker-compose

**Execução dos container redis**
```bash
docker-compose -f docker-compose.yml up
```
**Construção do projeto java**
```bash
./mvn clean build
```

**Comandos para execução no container kafka**
```bash
/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic streams-plaintext-input --partitions 1 --replication-factor 1
/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic streams-wordcount-output --partitions 1 --replication-factor 1
/bin/kafka-console-consumer --topic streams-wordcount-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
/bin/kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input
```

**Referências**
- https://github.com/confluentinc/kafka-streams-examples

**Faça bom proveito**