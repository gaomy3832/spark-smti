
SBTPATH = sbt
ifndef SPARKPATH
    $(error Spark top path 'SPARTPATH' is not set)
endif

CLASS_NAME = SMTITest
PROJECT_NAME = smti
PACKAGE_NAME = edu.stanford.cme323.spark.$(PROJECT_NAME)
JAR_FILE = $(wildcard target/scala-*/$(PROJECT_NAME)-assembly-*.jar)

# Set ARGS when run `make run'
# > make run ARGS="<argument> ..."
ARGS =

assembly:
	$(SBTPATH)/sbt assembly

run:
	$(SPARKPATH)/bin/spark-submit --class $(PACKAGE_NAME).$(CLASS_NAME) $(JAR_FILE) $(ARGS)


.PHONY: clean

clean:
	rm -rf target
	rm -rf project/target project/project
