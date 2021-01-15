all: build

build:
	mvn clean compile package

run:
	java -jar target/kafka-connect-restart-by-lag-0.1.0-SNAPSHOT.jar

package: build
	tar -czvf kafka-connect-restart-by-lag.tar.gz \
		target/kafka-connect-restart-by-lag-0.1.0-SNAPSHOT.jar \
		target/libs \
		Makefile \
		README.md