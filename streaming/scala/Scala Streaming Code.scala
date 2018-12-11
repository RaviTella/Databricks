// Databricks notebook source
// MAGIC %md 
// MAGIC ### Processing Time Series data from IoT Hub using Scala Structured Streaming DataFrames API. 
// MAGIC In this notebook we are going to lear how to use DataFrame API to build Structured Streaming applications. We want to compute real-time metrics like running counts and windowed counts on a stream of time series data.We will also send streaming updated to Power BI. 

// COMMAND ----------

// MAGIC %md
// MAGIC ###### Update the argumnet to ConnectionStringBuilder constructor with the end point of your IoT Hub
// MAGIC * You can find this information by clicking on the "Built-in endpoints" tab in your IoT Hub page in azure portal

// COMMAND ----------

    import org.apache.spark.eventhubs._

    // Build connection string 
    val connectionString = ConnectionStringBuilder("")
      .setEventHubName("rt-iothub")
      .build

    val customEventhubParameters =
      EventHubsConf(connectionString)
      .setConsumerGroup("")

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = StructType(Seq(
      StructField("processed", TimestampType, true),
      StructField("device_id", StringType, true),
      StructField("cycle", StringType, true),
      StructField("counter", StringType, true),
      StructField("endofcycle", StringType, true),      
      StructField("s9", StringType, true),
      StructField("s11", StringType, true),
      StructField("s14", StringType, true),
      StructField("s15", StringType, true)     
    ))

// COMMAND ----------

// MAGIC %md
// MAGIC ###### Upload reference data to Databricks cluster
// MAGIC * You can find the reference data in a file named "Engine_Master.csv" in Data folder of the IoT Hub client code
// MAGIC * Upload the file to the cluster via "Upload Data" option on the Azure Databricks home page for your cluster

// COMMAND ----------

  val refDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/FileStore/tables/Engine_Master.csv")
    refDF.show()  

// COMMAND ----------

import org.apache.spark.sql.functions._

val rawData = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

    val schematizedTelemetryData = rawData.select(from_json('body.cast("string"), schema) as "fields").select($"fields.*")

    val castedTelemetryData = schematizedTelemetryData
      .withColumn("processed", schematizedTelemetryData.col("processed").cast(TimestampType))
      .withColumn("device_id", schematizedTelemetryData.col("device_id"))
      .withColumn("cycle", schematizedTelemetryData.col("cycle"))
      .withColumn("counter", schematizedTelemetryData.col("counter"))
      .withColumn("endofcycle", schematizedTelemetryData.col("endofcycle"))
      .withColumn("s9", schematizedTelemetryData.col("s9").cast(FloatType))
      .withColumn("s11", schematizedTelemetryData.col("s11").cast(FloatType))
      .withColumn("s14", schematizedTelemetryData.col("s14").cast(FloatType))
      .withColumn("s15", schematizedTelemetryData.col("s15").cast(FloatType))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Send Telemetry to IoT Hub
// MAGIC * Run the IoT Hub client application from visual studio

// COMMAND ----------

    // Join with enginemaster reference data and print telemetry to console(Raw telemetry)
   import spark.implicits._
       castedTelemetryData
      .join(refDF, "device_id")
      .withWatermark("processed", "1 seconds")
      .select("device_id","cycle","counter","endofcycle","Model","State","City","CityState","s9","s11","s14","s15","Processed")
      .writeStream.outputMode("append")
      .format("console")
      .start().awaitTermination()

// COMMAND ----------

   // Join with enginemaster and print telemetry to console (aggregated at a 1-second window) 

    import spark.implicits._
       castedTelemetryData
      .join(refDF, "device_id")
      .withWatermark("processed", "1 minutes")
      .groupBy(
        window($"processed", "1 seconds"),
        $"processed", $"device_id", $"cycle", $"counter", $"endofcycle", $"Model",$"State",$"City",$"CityState" ).agg(avg("s9").alias("s9"),avg("s11").alias("s11"),avg("s14").alias("s14"),avg("s15").alias("s15"),max("processed").alias("processedTS"))
.select("device_id","cycle","counter","endofcycle","Model","State","City","CityState","s9","s11","s14","s15","ProcessedTS")
      .writeStream.outputMode("complete")
      .format("console")
      .start().awaitTermination()


// COMMAND ----------

// MAGIC %md
// MAGIC ###### Create and configure Power BI streaming data set
// MAGIC * Create Power BI streaming data set
// MAGIC  * <a href="https://docs.microsoft.com/en-us/power-bi/service-real-time-streaming" target="_blank">Streaming Data Set</a>
// MAGIC  * [{"avg_s11" :98.6,"alerts" :98.6}]
// MAGIC * Update the HttpPost constructor with the Power BI streaming endpoint

// COMMAND ----------

import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.HttpPost
import org.apache.http.HttpHeaders
import org.apache.http.entity.StringEntity
import com.google.gson.Gson

def sendToPBI(rowAsJson: String ): Unit = {
    val post = new HttpPost("")        
      post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")   
      post.setEntity(new StringEntity(rowAsJson))
      val response = (HttpClientBuilder.create().build()).execute(post)
    }

// COMMAND ----------

//Send alerts to Power BI streaming endpoint when the avg s11 value exceeds the alert value.  This query is run every 5 seconds
import org.apache.spark.sql._
 castedTelemetryData
      .join(refDF, "device_id")
      .withWatermark("processed", "5 seconds")
      .groupBy(
        window($"processed", "5 seconds"),
        $"device_id",$"Model",$"State",$"City",$"CityState" ).agg(max("processed").alias("processedTS"),avg("s11").alias("avg_s11"),count("*").alias("alerts")).where("avg_s11 > 47.00")
     .select("avg_s11","alerts")
     .writeStream.outputMode("complete").foreachBatch { (batchDF: DataFrame, batchId: Long) =>     
        batchDF.show      
        batchDF.toJSON.foreach { row =>
         sendToPBI(row) 
        }    
    }.start().awaitTermination()
    
