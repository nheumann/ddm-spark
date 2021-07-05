package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val datasets = inputs.map(file => {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(file)
    })
    println(datasets.length)

    val cells = datasets.flatMap(data => data.columns.map(data.select(_)))
      .map(col => col
      .map(row => (row.get(0).toString, row.schema.fieldNames.head)))

    val df = cells.reduce(_ union _)
      .toDF("value", "column_name")
    df.show(false)

    val attributeGroups = df.groupBy("value").agg(collect_set(col("column_name")).as("column_names"))
      .select("column_names")
      .distinct()
      .collect().map(_(0)).toList


    print(attributeGroups)
    // val indCandidates = attributeGroups.













  }
}
