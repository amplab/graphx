# Apache Spark

Lightning-Fast Cluster Computing - <http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the project webpage at <http://spark.apache.org/documentation.html>.
This README file only contains basic setup instructions.


## Building

Spark requires Scala 2.10. The project is built using Simple Build Tool (SBT),
which can be obtained [here](http://www.scala-sbt.org). If SBT is installed we
will use the system version of sbt otherwise we will attempt to download it
automatically. To build Spark and its example programs, run:

    ./sbt/sbt assembly

Once you've built Spark, the easiest way to start using it is the shell:

    ./bin/spark-shell

Or, for the Python API, the Python shell (`./bin/pyspark`).

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> <params>`. For example:

    ./bin/run-example org.apache.spark.examples.SparkLR local[2]

will run the Logistic Regression example locally on 2 CPUs.

Each of the example programs prints usage help if no params are given.

All of the Spark samples take a `<master>` parameter that is the cluster URL
to connect to. This can be a mesos:// or spark:// URL, or "local" to run
locally with one thread, or "local[N]" to run locally with N threads.

## Running tests

Testing first requires [Building](#building) Spark. Once Spark is built, tests
can be run using:

`./sbt/sbt test`
 
## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting the `SPARK_HADOOP_VERSION` environment
when building Spark.

For Apache Hadoop versions 1.x, Cloudera CDH MRv1, and other Hadoop
versions without YARN, use:

    # Apache Hadoop 1.2.1
    $ SPARK_HADOOP_VERSION=1.2.1 sbt/sbt assembly

    # Cloudera CDH 4.2.0 with MapReduce v1
    $ SPARK_HADOOP_VERSION=2.0.0-mr1-cdh4.2.0 sbt/sbt assembly

For Apache Hadoop 2.2.X, 2.1.X, 2.0.X, 0.23.x, Cloudera CDH MRv2, and other Hadoop versions
with YARN, also set `SPARK_YARN=true`:

    # Apache Hadoop 2.0.5-alpha
    $ SPARK_HADOOP_VERSION=2.0.5-alpha SPARK_YARN=true sbt/sbt assembly

    # Cloudera CDH 4.2.0 with MapReduce v2
    $ SPARK_HADOOP_VERSION=2.0.0-cdh4.2.0 SPARK_YARN=true sbt/sbt assembly

    # Apache Hadoop 2.2.X and newer
    $ SPARK_HADOOP_VERSION=2.2.0 SPARK_YARN=true sbt/sbt assembly

When developing a Spark application, specify the Hadoop version by adding the
"hadoop-client" artifact to your project's dependencies. For example, if you're
using Hadoop 1.2.1 and build your application using SBT, add this entry to
`libraryDependencies`:

    "org.apache.hadoop" % "hadoop-client" % "1.2.1"

If your project is built with Maven, add this to your POM file's `<dependencies>` section:

    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>1.2.1</version>
    </dependency>


## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.


## Contributing to Spark

Contributions via GitHub pull requests are gladly accepted from their original
author. Along with any pull requests, please state that the contribution is
your original work and that you license the work to the project under the
project's open source license. Whether or not you state this explicitly, by
submitting any copyrighted material via pull request, email, or other means
you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.


## Books

[Spark GraphX in Action](https://manning.com/books/spark-graphx-in-action) by Michael S. Malak and Robin East (Manning Publications). [Chapter 1](https://manning-content.s3.amazonaws.com/download/4/442be53-a723-4be9-8139-6d4adf5a5f59/SparkGraphXinA_MEAP_ch1.pdf)
