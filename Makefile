
SBTPATH = sbt
ifndef SPARKPATH
    $(error Spark top path 'SPARTPATH' is not set)
endif

CLASS_NAME = MainApp
PROJECT_NAME = smti
JAR_FILE = $(wildcard target/scala-*/$(PROJECT_NAME)-assembly-*.jar)

N = 4
ARGS =

assembly:
	$(SBTPATH)/sbt assembly

run:
	$(SPARKPATH)/bin/spark-submit --master local[$N] --class $(CLASS_NAME) $(JAR_FILE) $(ARGS)


.PHONY: clean

clean:
	rm -rf target
	rm -rf project/target project/project
