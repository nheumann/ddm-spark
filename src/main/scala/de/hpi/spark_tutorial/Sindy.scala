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

    // joining of attribute groups
    val attributeGroups = df.groupBy("value").agg(collect_set(col("column_name")).as("column_names"))
      .select("column_names")
      .distinct()
      .as[Seq[String]]
      /*
      .select("column_names")
      .distinct()
      .collect().map(_(0)).toList
      */
    println(attributeGroups)

    val indCandidates = attributeGroups
      .select(explode(attributeGroups("column_names")).as("key"), col("column_names"))
      .as[(String, Seq[String])]
      .map(c => (c._1, c._2.filter(_ != c._1)) )
      .toDF("lhs", "rhs")
    indCandidates.show()

    val groupedIndCandidates = indCandidates
      .groupBy(indCandidates("lhs")).agg(collect_set(indCandidates("rhs")))
      .as[(String,Seq[Seq[String]])]
      .map(c => (c._1,c._2.reduce(_.intersect(_))))
      .filter(c=> c._2.nonEmpty)

    groupedIndCandidates.show()

    groupedIndCandidates.foreach(c => println(c._1 + " <= " + c._2.reduce(_ + ", " + _)) )


    // val test1 = attributeGroups.map(arr => explode(arr))
    // println(test1)

    // val indCandidates = attributeGroups.map(group => group.map(attribute => (attribute, )))













  }
}
