package org.apache.spark

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import com.typesafe.config.ConfigFactory

/**
 * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
 *
 * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
 * values from both the `spark.*` Java system properties and any `spark.conf` on your application's
 * classpath (if it has one). In this case, system properties take priority over `spark.conf`, and
 * any parameters you set directly on the `SparkConf` object take priority over both of those.
 *
 * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
 * get the same configuration no matter what is on the classpath.
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new SparkConf().setMaster("local").setAppName("My app")`.
 *
 * Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
 * by the user. Spark does not support modifying the configuration at runtime.
 *
 * @param loadDefaults whether to load values from the system properties and classpath
 */
class SparkConf(loadDefaults: Boolean) extends Serializable with Cloneable with Logging {

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new HashMap[String, String]()

  if (loadDefaults) {
    ConfigFactory.invalidateCaches()
    val typesafeConfig = ConfigFactory.systemProperties()
      .withFallback(ConfigFactory.parseResources("spark.conf"))
    for (e <- typesafeConfig.entrySet().asScala if e.getKey.startsWith("spark.")) {
      settings(e.getKey) = e.getValue.unwrapped.toString
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    this
  }

  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** Set a name for your application. Shown in the Spark web UI. */
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  /** Set JAR files to distribute to the cluster. */
  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
    set("spark.jars", jars.filter(_ != null).mkString(","))
  }

  /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toSeq)
  }

  /**
   * Set an environment variable to be used when launching executors for this application.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
   */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set("spark.executorEnv." + variable, value)
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
   */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * (Java-friendly version.)
   */
  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toSeq)
  }

  /**
   * Set the location where Spark is installed on worker nodes.
   */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (!settings.contains(key)) {
      settings(key) = value
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    settings.get(key)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = settings.clone().toArray

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll.filter{case (k, v) => k.startsWith(prefix)}
          .map{case (k, v) => (k.substring(prefix.length), v)}
  }

  /** Get all akka conf variables set on this SparkConf */
  def getAkkaConf: Seq[(String, String)] =  getAll.filter {case (k, v) => k.startsWith("akka.")}

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.contains(key)

  /** Copy this object */
  override def clone: SparkConf = {
    new SparkConf(false).setAll(settings)
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
}
