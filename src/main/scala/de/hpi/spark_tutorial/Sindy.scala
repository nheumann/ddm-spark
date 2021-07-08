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

    // create cells of datasets
    val cells = datasets.flatMap(data => data.columns.map(data.select(_)))
      .map(col => col
      .map(row => (row.get(0).toString, row.schema.fieldNames.head)))

    // joining of attribute groups
    val attributeGroups = cells.reduce(_ union _)
      .toDF("value", "column_name")
      .groupBy("value").agg(collect_set(col("column_name")).as("column_names"))
      .select("column_names")
      .distinct()
      .as[Seq[String]]

    // compute all ind candidates from attribute groups
    val indCandidates = attributeGroups
      .select(explode(attributeGroups("column_names")).as("key"), col("column_names"))
      .as[(String, Seq[String])]
      .map(c => (c._1, c._2.filter(_ != c._1)) )
      .toDF("lhs", "rhs")

    // group and sort all ind candidates by left hand side
    val groupedIndCandidates = indCandidates
      .groupBy(indCandidates("lhs")).agg(collect_set(indCandidates("rhs")))
      .as[(String,Seq[Seq[String]])]
      .map(c => (c._1,c._2.reduce(_.intersect(_))))
      .filter(c=> c._2.nonEmpty)
      .toDF("lhs", "rhs")
      .orderBy(col("lhs"))
      .as[(String,Seq[String])]

    // print the inds
    groupedIndCandidates.collect().foreach(c => println(c._1 + " < " + c._2.reduce(_ + ", " + _)) )
  }

}
