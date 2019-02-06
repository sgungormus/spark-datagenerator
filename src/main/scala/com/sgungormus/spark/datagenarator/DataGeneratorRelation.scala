package com.sgungormus.spark.datagenarator

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.math.BigDecimal


class DataGeneratorRelation(
                                override val sqlContext: SQLContext,
                                records: Int,
                                userSchema: StructType)
  extends BaseRelation
    with TableScan with PrunedScan with PrunedFilteredScan with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("name", StringType, nullable = true) ::
          StructField("surname", StringType, nullable = true) ::
          StructField("salary", IntegerType, nullable = true) ::
          Nil
      )
    }
  }

  override def buildScan(): RDD[Row] = {
    generateRandomRDD(sqlContext.sparkContext,schema,records)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    generateRandomRDD(sqlContext.sparkContext,schema,records)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    generateRandomRDD(sqlContext.sparkContext,schema,records)
  }


  /* Code required to create a random rdd with given schema */

  def generateRandomRDD(
                         sc: SparkContext,
                         userSchema: StructType,
                         numRecords: Int
                       ) : RDD[Row] = {

    var partitions:Int = 1

    numRecords match {
      case x if x <=100 => partitions = 1
      case x if x <= 1000 => partitions = 5
      case x if x <= 10000 => partitions = 10
      case x if x <= 100000 => partitions = 20
      case x if x <= 1000000 => partitions = 50
      case x if x <= 10000000 => partitions = 100
      case _ => partitions = 250
    }

    val recordsPerPartition = numRecords / partitions

    val difference = numRecords%partitions
    val newRDD = sc.parallelize(Seq.fill(1)(difference), 1)
    val unionRDD = sc.parallelize(Seq.fill(partitions)(recordsPerPartition), partitions)
    val dfRDD = unionRDD.union(newRDD)

    val rdd = dfRDD.flatMap(item => {
      Seq.fill(item)(getRandomRow(userSchema))
    })
    rdd

  }

  def randomBooleanForNull() : Boolean = {
    val r = scala.util.Random
    val number = r.nextInt(10)
    if (number == 8 || number == 9) {
      return true
    } else {
      return false
    }
  }


  def getRandomRow(userSchema:StructType): Row = {



    val schemaSize = userSchema.fields.length

    val rowArray = new Array[Any](schemaSize)

    var index: Int = 0

    index = 0
    while (index < schemaSize) {

      val field = userSchema.fields(index)
      val dataType = field.dataType
      val nullable = field.nullable
      dataType match {
        case _: ByteType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomByte()
          }
        }
        case _: ShortType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomShort()
          }
        }
        case _: IntegerType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomInt()
          }
        }
        case _: LongType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomLong()
          }
        }
        case _: FloatType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomFloat()
          }
        }
        case _: DoubleType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomDouble()
          }
        }
        case _: BinaryType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomBinary()
          }
        }
        case _: BooleanType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomBoolean()
          }
        }
        case _: DecimalType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomDecimal(field.metadata.getString("precision").toInt, field.metadata.getString("scale").toInt)
          }
        }
        case _: TimestampType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomTimestamp()
          }
        }
        case _: DateType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomDate()
          }
        }
        case _: StringType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            rowArray(index) = randomString(20)
          }
        }
        case _: StructType => {
          val innerSchema = field.dataType.asInstanceOf[StructType]
          rowArray(index) = getRandomRow(innerSchema)
        }
        case _: ArrayType => {
          if(randomBooleanForNull() && nullable) {
            rowArray(index) = null
          } else {
            val innerType = field.dataType.asInstanceOf[ArrayType].elementType
            rowArray(index) = randomArray(field, innerType)
          }
        }
        case _ => throw new RuntimeException(s"Unsupported Data Type")
      }
      index = index + 1
    }
    Row.fromSeq(rowArray)
  }

  def randomArray(field: StructField, dt: DataType) : Seq[Any] = {
    val arraySize:Int = 3
    //val elementNullable = field.metadata.getString("elementNullable").toBoolean
    val elementNullable = false

    dt match {
      case _: ByteType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toByte)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomByte())
        }
      }
      case _: ShortType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toShort)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomShort())
        }
      }
      case _: IntegerType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toInt)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomInt())
        }
      }
      case _: LongType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toLong)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomLong())
        }
      }
      case _: FloatType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toFloat)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomFloat())
        }
      }
      case _: DoubleType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toDouble)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomDouble())
        }
      }
      case _: BinaryType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)((field.metadata.getString("elementNullReplacement").toInt).toBinaryString)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomBinary())
        }
      }
      case _: BooleanType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            Seq.fill(arraySize)(field.metadata.getString("elementNullReplacement").toBoolean)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomBoolean())
        }
      }
      case _: DecimalType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            val decimal = BigDecimal(field.metadata.getString("elementNullReplacement").toLong , field.metadata.getString("scale").toInt)
            Seq.fill(arraySize)(decimal)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomDecimal(field.metadata.getString("precision").toInt, field.metadata.getString("scale").toInt))
        }
      }
      case _: TimestampType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            val time = new Timestamp(field.metadata.getString("elementNullReplacement").toLong)
            Seq.fill(arraySize)(time)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomTimestamp())
        }
      }
      case _: DateType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            val date = new Date (field.metadata.getString("elementNullReplacement").toLong)
            Seq.fill(arraySize)(date)
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomDate())
        }
      }
      case _: StringType => {
        if(randomBooleanForNull() && elementNullable) {
          try {
            val replacement = field.metadata.getString("nullReplacement").toString
            if (replacement != "%00") {
              Seq.fill(arraySize)(replacement)
            } else {
              Seq.fill(arraySize)(null)
            }
          } catch {
            case e: Exception => Seq.fill(arraySize)(null)
          }
        } else {
          Seq.fill(arraySize)(randomString(20))
        }
      }
      case _: StructType => {
        val innerSchema = dt.asInstanceOf[StructType]
        Seq.fill(arraySize)(getRandomRow(innerSchema))
      }
      case _: ArrayType => {
        val innerType = dt.asInstanceOf[ArrayType].elementType
        Seq.fill(1)(randomArray(field, innerType))
      }
      case _ => throw new RuntimeException(s"Unsupported Data Type")
    }
  }

  def randomLong() : Long = {
    val r = scala.util.Random
    return r.nextLong()
  }

  def randomInt() : Int = {
    val r = scala.util.Random
    return r.nextInt()
  }

  def randomFloat() : Float = {
    val r = scala.util.Random
    return r.nextFloat()
  }

  def randomDouble() : Double = {
    val r = scala.util.Random
    return r.nextDouble()
  }

  def randomBinary() : String = {
    val r = scala.util.Random
    return r.nextInt().toBinaryString
  }

  def randomShort() : Short = {
    val r = scala.util.Random
    return r.nextInt().toShort
  }

  def randomByte() : Byte = {
    val r = scala.util.Random
    return r.nextInt().toByte
  }

  def randomBoolean() : Boolean = {
    val r = scala.util.Random
    if (r.nextInt(2) == 1) {
      return true
    } else {
      return false
    }
  }

  def randomDecimal(precision : Int, scale : Int) : BigDecimal = {
    val r = scala.util.Random
    var string = ""
    for( a <- 1 to precision){
      string = string + r.nextInt(10).toString
    }
    return BigDecimal(string.toLong , scale)
  }

  def randomLetter() : Char = {
    val r = scala.util.Random
    val low = 65
    val high = 122
    val x = (r.nextInt(high - low) + low)
    if (x >= 91 && x<=96) {
      randomLetter()
    } else {
      return x.toChar
    }
  }

  def randomString(limit: Int) : String = {
    val r = scala.util.Random
    var z = new Array[Char](limit)
    var string = ""
    for( a <- 0 to limit-1){
      z(a) = randomLetter()
      string = string + z(a)
    }
    return string
  }


  def randomTimestamp () : Timestamp = {
    val r = scala.util.Random
    var string = ""
    for( a <- 1 to 9){
      string = string + r.nextInt(10).toString
    }
    string = r.nextInt(2) + string
    val timestamp = new Timestamp(string.toLong * 1000)
    return timestamp
  }

  def randomDate () : Date = {
    val date = new Date(randomTimestamp().getTime())
    return date
  }

}
