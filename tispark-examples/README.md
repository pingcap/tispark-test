# TiSpark Examples

## How To Use
Build with maven:
```
mvn clean install
```
Run example with spark-submit, for example, run `JavaExample`:
```scala
spark-submit --master local[*] --class com.pingcap.tispark.examples.JavaExample ./target/tispark-examples-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```