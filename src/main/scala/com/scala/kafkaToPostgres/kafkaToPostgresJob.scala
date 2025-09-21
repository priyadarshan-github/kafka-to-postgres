package com.scala.kafkaToPostgres

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.sql.DriverManager

object kafkaToPostgresJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToPostgresPipeline")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define schema properly
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("email", StringType, true)
    ))

    // Read from Confluent Kafka (change below settings accordingly)
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "personal-info")
      .option("startingOffsets", "earliest") // or latest
      .load()

    val jsonDF = kafkaDF.selectExpr("CAST(value AS STRING) as json")

    //jsonDF.show()

    val parsedDF = jsonDF.select(from_json($"json", schema).as("data")).select("data.*")


    val stagedDF = parsedDF
      .withColumn("status", lit("STAGED"))
      .withColumn("ingestion_ts", current_timestamp())

    stagedDF.printSchema()
//    stagedDF.show(false)
//    stagedDF.show()
//    System.exit(0)

    // Postgres connection
    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    val dbProps = new java.util.Properties()
    dbProps.setProperty("user", "postgres")
    dbProps.setProperty("password", "postgres")
    dbProps.setProperty("driver", "org.postgresql.Driver")


//    stagedDF.writeStream
//      .foreachBatch { (fDF: Dataset[Row], _: Long) =>
//
//        println("=== Schema ===")
//        println(fDF.schema.treeString)
//        fDF.show(20, truncate = false)
//
//      }
//      .start()
//      .awaitTermination()
//    System.exit(0)

    stagedDF.writeStream
      .foreachBatch { (finalDF: Dataset[Row], _: Long) =>
        finalDF.write
          .mode("append")
          .jdbc(jdbcUrl, "stage_table", dbProps)

        val stageDF = spark.read
          .jdbc(jdbcUrl, "(SELECT * FROM stage_table WHERE status = 'STAGED') AS t", dbProps)
        stageDF.createOrReplaceTempView("staging_view")

        val masterDF = spark.read
          .jdbc(jdbcUrl, "master_table", dbProps)
        masterDF.createOrReplaceTempView("master_view")

        val insertDF = spark.sql(
          """
            |SELECT s.* FROM staging_view s
            |LEFT JOIN master_view m ON s.id = m.id
            |WHERE m.id IS NULL
          """.stripMargin)

        insertDF.write
          .mode("append")
          .jdbc(jdbcUrl, "master_table", dbProps)

        val successfulIds = insertDF.select("id").as[Int].collect().mkString(",")
        if (successfulIds.nonEmpty) {
          val conn = DriverManager.getConnection(jdbcUrl, dbProps)
          val stmt = conn.createStatement()
          stmt.executeUpdate(
            s"UPDATE stage_table SET status = 'SUCCESS', processed_ts = CURRENT_TIMESTAMP WHERE id IN ($successfulIds)"
          )
          stmt.close()
          conn.close()
        }
      }
      .start()
      .awaitTermination()
  }
}