# spark-datagenerator
A custom datasource for Spark to create a sample dataframe with given schema. 

## Options
This package allows creating dataframes with random data. User defined schema will be used to determine the structure of the dataframe. 
Following options are available:
* `records`: Number of records to generate (default 10)

### Usage

You can manually specify the schema when reading data:

```scala
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

val mySchema = StructType(Array(
    StructField("age", IntegerType, true),
    StructField("name", StringType, true),
    StructField("city", StringType, true),
    StructField("country", StringType, true),
    StructField("isValid", BooleanType, true)))

val df = spark.read
    .format("com.sgungormus.spark.datagenarator")
    .option("records", "500")
    .schema(mySchema)
    .load()
    
df.show()
df.count()

```
