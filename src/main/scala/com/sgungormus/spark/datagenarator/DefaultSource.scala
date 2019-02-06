package com.sgungormus.spark.datagenarator

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  // Method that comes from RelationProvider.
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    createRelation(sqlContext, parameters, null)
  }

  // Method that comes from SchemaRelationProvider, which allows users to specify the schema.
  // In this case, we do not need to discover it on our own.
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType): BaseRelation = {

    val records = Integer.parseInt(parameters.getOrElse("records", "10"))
    new DataGeneratorRelation(sqlContext, records, schema)

  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {


    sys.error("DataGenerator is a readonly datasource!"); sys.exit(1)
  }

}