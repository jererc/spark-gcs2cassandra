package com.jererc

import java.io.File
import scala.io.Source
import scala.util.Try
import scala.util.matching.Regex

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import net.liftweb.json._

import com.eaio.uuid.UUID;

object CassandraTable {
  val columns = SomeColumns(
    "uuid",
    "timestamp",
    "count",
    "error_code",
    "metric_inventory",
    "metric_impression",
    "metric_click",
    "metric_companion",
    "metric_companion_click",
    "metric_close",
    "metric_skip",
    "metric_view25",
    "metric_view50",
    "metric_view75",
    "metric_view100",
    "ad_position",
    "ad_muted",
    "ad_autoplay",
    "ad_revenue",
    "ad_source_type",
    "ad_source_connection",
    "ad_type",
    "creative_id",
    "creative_name",
    "creative_duration",
    "campaign_id",
    "campaign_name",
    "advertiser_category",
    "advertiser_name",
    "order_name",
    "order_id",
    "order_line_id",
    "order_line_name",
    "order_number",
    "inventory_scenario_id",
    "inventory_scenario_name",
    "segment_id",
    "krux_segment_code",
    "krux_segment_name",
    "visitor_liverail_id",
    "visitor_country",
    "visitor_city",
    "visitor_browser",
    "visitor_platform",
    "visitor_ip",
    "visitor_lang",
    "visitor_carrier",
    "visitor_domain",
    "visitor_mobile_make",
    "visitor_player_size",
    "video_id",
    "video_upload_date",
    "video_type",
    "video_stream_mode",
    "video_duration",
    "video_duration_segment",
    "video_category",
    "video_private",
    "video_owner_id",
    "video_owner_username",
    "video_owner_registration_date",
    "video_owner_country",
    "video_owner_parent_id",
    "video_owner_parent_username",
    "video_claimer_id",
    "video_claimer_username",
    "video_page_id",
    "video_page_type",
    "video_player_type",
    "video_isrc",
    "video_upc",
    "publisher_id",
    "publisher_liverail_id",
    "publisher_name",
    "publisher_site",
    "publisher_country"
  )
}

class CassandraData (
  val uuid: java.util.UUID,
  val timestamp: Long,
  val count: Int,
  val error_code: Int,
  val metric_inventory: Boolean,
  val metric_impression: Boolean,
  val metric_click: Boolean,
  val metric_companion: Boolean,
  val metric_companion_click: Boolean,
  val metric_close: Boolean,
  val metric_skip: Boolean,
  val metric_view25: Boolean,
  val metric_view50: Boolean,
  val metric_view75: Boolean,
  val metric_view100: Boolean,
  val ad_position: String,
  val ad_muted: Boolean,
  val ad_autoplay: String,
  val ad_revenue: Double,
  val ad_source_type: String,
  val ad_source_connection: String,
  val ad_type: String,
  val creative_id: String,
  val creative_name: String,
  val creative_duration: String,
  val campaign_id: String,
  val campaign_name: String,
  val advertiser_category: String,
  val advertiser_name: String,
  val order_name: String,
  val order_id: String,
  val order_line_id: String,
  val order_line_name: String,
  val order_number: String,
  val inventory_scenario_id: String,
  val inventory_scenario_name: String,
  val segment_id: String,
  val krux_segment_code: String,
  val krux_segment_name: String,
  val visitor_liverail_id: String,
  val visitor_country: String,
  val visitor_city: String,
  val visitor_browser: String,
  val visitor_platform: String,
  val visitor_ip: String,
  val visitor_lang: String,
  val visitor_carrier: String,
  val visitor_domain: String,
  val visitor_mobile_make: String,
  val visitor_player_size: String,
  val video_id: Int,
  val video_upload_date: String,
  val video_type: String,
  val video_stream_mode: String,
  val video_duration: Int,
  val video_duration_segment: String,
  val video_category: String,
  val video_private: String,
  val video_owner_id: Int,
  val video_owner_username: String,
  val video_owner_registration_date: String,
  val video_owner_country: String,
  val video_owner_parent_id: String,
  val video_owner_parent_username: String,
  val video_claimer_id: String,
  val video_claimer_username: String,
  val video_page_id: String,
  val video_page_type: String,
  val video_player_type: String,
  val video_isrc: String,
  val video_upc: String,
  val publisher_id: String,
  val publisher_liverail_id: String,
  val publisher_name: String,
  val publisher_site: String,
  val publisher_country: String
) extends Serializable

object CassandraData {

  def getBoolean(value: String) =
    Try(value.toBoolean).getOrElse(false)

  def getInt(value: String) =
    Try(value.toInt).getOrElse(0)

  def getLong(value: String) =
    Try(value.toLong).getOrElse(0L)

  def getDouble(value: String) =
    Try(value.toDouble).getOrElse(0D)

  def apply(row: Seq[String]) = {
    val uuid = java.util.UUID.randomUUID
    // val uuid = java.util.UUID.fromString(new com.eaio.uuid.UUID().toString())
    new CassandraData(
      uuid,
      getLong(row(0)) * 1000,    // timestamp (Long)
      getInt(row(1)),    // count (Int)
      getInt(row(2)),    // error_code (Int)
      getBoolean(row(3)),    // metric_inventory (Boolean)
      getBoolean(row(4)),    // metric_impression (Boolean)
      getBoolean(row(5)),    // metric_click (Boolean)
      getBoolean(row(6)),    // metric_companion (Boolean)
      getBoolean(row(7)),    // metric_companion_click (Boolean)
      getBoolean(row(8)),    // metric_close (Boolean)
      getBoolean(row(9)),    // metric_skip (Boolean)
      getBoolean(row(10)),    // metric_view25 (Boolean)
      getBoolean(row(11)),    // metric_view50 (Boolean)
      getBoolean(row(12)),    // metric_view75 (Boolean)
      getBoolean(row(13)),    // metric_view100 (Boolean)
      row(14),    // ad_position (String)
      getBoolean(row(15)),    // ad_muted (Boolean)
      row(16),    // ad_autoplay (String)
      getDouble(row(17)),    // ad_revenue (Double)
      row(18),    // ad_source_type (String)
      row(19),    // ad_source_connection (String)
      row(20),    // ad_type (String)
      row(21),    // creative_id (String)
      row(22),    // creative_name (String)
      row(23),    // creative_duration (String)
      row(24),    // campaign_id (String)
      row(25),    // campaign_name (String)
      row(26),    // advertiser_category (String)
      row(27),    // advertiser_name (String)
      row(28),    // order_name (String)
      row(29),    // order_id (String)
      row(30),    // order_line_id (String)
      row(31),    // order_line_name (String)
      row(32),    // order_number (String)
      row(33),    // inventory_scenario_id (String)
      row(34),    // inventory_scenario_name (String)
      row(35),    // segment_id (String)
      row(36),    // krux_segment_code (String)
      row(37),    // krux_segment_name (String)
      row(38),    // visitor_liverail_id (String)
      row(39),    // visitor_country (String)
      row(40),    // visitor_city (String)
      row(41),    // visitor_browser (String)
      row(42),    // visitor_platform (String)
      row(43),    // visitor_ip (String)
      row(44),    // visitor_lang (String)
      row(45),    // visitor_carrier (String)
      row(46),    // visitor_domain (String)
      row(47),    // visitor_mobile_make (String)
      row(48),    // visitor_player_size (String)
      getInt(row(49)),    // video_id (Int)
      row(50),    // video_upload_date (String)
      row(51),    // video_type (String)
      row(52),    // video_stream_mode (String)
      getInt(row(53)),    // video_duration (Int)
      row(54),    // video_duration_segment (String)
      row(55),    // video_category (String)
      row(56),    // video_private (String)
      getInt(row(57)),    // video_owner_id (Int)
      row(58),    // video_owner_username (String)
      row(59),    // video_owner_registration_date (String)
      row(60),    // video_owner_country (String)
      row(61),    // video_owner_parent_id (String)
      row(62),    // video_owner_parent_username (String)
      row(63),    // video_claimer_id (String)
      row(64),    // video_claimer_username (String)
      row(65),    // video_page_id (String)
      row(66),    // video_page_type (String)
      row(67),    // video_player_type (String)
      row(68),    // video_isrc (String)
      row(69),    // video_upc (String)
      row(70),    // publisher_id (String)
      row(71),    // publisher_liverail_id (String)
      row(72),    // publisher_name (String)
      row(73),    // publisher_site (String)
      row(74)    // publisher_country (String)
    )
  }
}

object Gcs2Cassandra {

  val defaultConf = """{
    "cassandraHost" : "localhost",
    "cassandraRpcPort" : "9160",
    "cassandraKeyspace" : "test_keyspace",
    "cassandraTable" : "test_table_output",
    "sparkMasterHost" : "localhost",
    "sparkMasterPort" : "7077",
    "gcsProjectId" : "",
    "gcsAccountEmail" : "",
    "gcsAccountKeyfile" : "",
    "gcsFilePrefix" : "",
    "resetCassandraKeyspace" : false,
    "resetCassandraTable" : false
  }"""

  implicit val formats = DefaultFormats
  case class Conf(
    cassandraHost: String,
    cassandraRpcPort: String,
    cassandraKeyspace: String,
    cassandraTable: String,
    sparkMasterHost: String,
    sparkMasterPort: String,
    gcsProjectId : String,
    gcsAccountEmail : String,
    gcsAccountKeyfile : String,
    gcsFilePrefix : String,
    resetCassandraKeyspace : Boolean,
    resetCassandraTable : Boolean
  )

  def recursiveListFiles(f: File, r: Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
  }

  def main(args: Array[String]) {
    val jsonConf = parse(defaultConf) merge parse(Source.fromFile("local_settings.json").getLines.mkString)
    val conf = jsonConf.extract[Conf]

    val sparkConf = new SparkConf(true)
    // Cassandra connector settings
    sparkConf.set("spark.cassandra.connection.host", conf.cassandraHost)
    sparkConf.set("spark.cassandra.connection.rpc.port", conf.cassandraRpcPort)
    // sparkConf.set("spark.cassandra.output.batch.size.rows", "100")
    // sparkConf.set("spark.cassandra.output.batch.size.bytes", s"${32 * 1024}")
    // sparkConf.set("spark.cassandra.output.concurrent.writes", "32")

    // sparkConf.set("spark.executor.memory", "2g")

    // We push this assembly into spark
    val assemblies = recursiveListFiles(new File("target"), """\bassembly\b.*\.jar$""".r).map(f => f.getAbsolutePath)
    sparkConf.setJars(assemblies)

    // Init Cassandra
    val connector = CassandraConnector(sparkConf)
    if (conf.resetCassandraKeyspace) {
      connector.withSessionDo { session =>
        session.execute(s"DROP KEYSPACE IF EXISTS ${conf.cassandraKeyspace};")
        session.execute(s"CREATE KEYSPACE ${conf.cassandraKeyspace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
      }
      println(s"reset keyspace ${conf.cassandraKeyspace}")
    }
    if (conf.resetCassandraTable) {
      connector.withSessionDo { session =>
        session.execute(s"DROP TABLE IF EXISTS ${conf.cassandraKeyspace}.${conf.cassandraTable};")
        session.execute(s"CREATE TABLE ${conf.cassandraKeyspace}.${conf.cassandraTable} (uuid uuid, timestamp timestamp, count int, error_code int, metric_inventory boolean, metric_impression boolean, metric_click boolean, metric_companion boolean, metric_companion_click boolean, metric_close boolean, metric_skip boolean, metric_view25 boolean, metric_view50 boolean, metric_view75 boolean, metric_view100 boolean, ad_position text, ad_muted boolean, ad_autoplay text, ad_revenue double, ad_source_type text, ad_source_connection text, ad_type text, creative_id text, creative_name text, creative_duration text, campaign_id text, campaign_name text, advertiser_category text, advertiser_name text, order_name text, order_id text, order_line_id text, order_line_name text, order_number text, inventory_scenario_id text, inventory_scenario_name text, segment_id text, krux_segment_code text, krux_segment_name text, visitor_liverail_id text, visitor_country text, visitor_city text, visitor_browser text, visitor_platform text, visitor_ip text, visitor_lang text, visitor_carrier text, visitor_domain text, visitor_mobile_make text, visitor_player_size text, video_id int, video_upload_date text, video_type text, video_stream_mode text, video_duration int, video_duration_segment text, video_category text, video_private text, video_owner_id int, video_owner_username text, video_owner_registration_date text, video_owner_country text, video_owner_parent_id text, video_owner_parent_username text, video_claimer_id text, video_claimer_username text, video_page_id text, video_page_type text, video_player_type text, video_isrc text, video_upc text, publisher_id text, publisher_liverail_id text, publisher_name text, publisher_site text, publisher_country text, PRIMARY KEY ( uuid ) );")
      }
      println(s"reset table ${conf.cassandraKeyspace}.${conf.cassandraTable}")
    }

    val gcsFile = s"gs://${conf.gcsProjectId}${conf.gcsFilePrefix}"

    val sc = new SparkContext(s"spark://${conf.sparkMasterHost}:${conf.sparkMasterPort}",
      gcsFile, sparkConf)

    // GCS connector settings
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    sc.hadoopConfiguration.set("fs.gs.project.id", conf.gcsProjectId)
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email", conf.gcsAccountEmail)
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.keyfile", conf.gcsAccountKeyfile)

    // Map CSV lines
    val gcsRdd = sc.textFile(gcsFile)
    val dataRdd = gcsRdd
      .map(l => l.split(",", -1))
      .filter(l => l(0) != "timestamp")
      .map(l => CassandraData(l))

    val count = dataRdd.count()

    // Write to the table
    println(s"Inserting $count lines...")
    val ts_begin: Long = System.currentTimeMillis / 1000
    dataRdd.saveToCassandra(conf.cassandraKeyspace, conf.cassandraTable,
      CassandraTable.columns)
    val ts_end: Long = System.currentTimeMillis / 1000
    val elapsed = ts_end - ts_begin

    sc.stop()

    println(s"Inserted $count lines into ${conf.cassandraKeyspace}.${conf.cassandraTable} in $elapsed sec (${count / elapsed} lines / sec)")
  }
}
