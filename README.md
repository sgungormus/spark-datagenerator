# spark-datagenerator
A custom datasource for Spark to create a sample dataframe with given schema. 

## Options
This package allows creating dataframes with random data. User defined schema will be used to determine the structure of the dataframe. 
Following options are available:
* `records`: Number of records to generate (default 10)

### Usage

You can manually specify the schema when reading data:

```sh
>spark-shell --jars spark-data-generator_2.11-0.1.jar
```


```scala

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, BooleanType}

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
    .persist()
    
df.show()
+-----------+--------------------+--------------------+--------------------+-------+
|        age|                name|                city|             country|isValid|
+-----------+--------------------+--------------------+--------------------+-------+
|  477490172|ASYCXsqMuQQExCjlqnby|CXyvbpXSqrBkOKDkxPkr|ycnIJWxDwPobRcnjNiYw|   true|
|       null|                null|XcnAQmaokiQjvxBesCgT|ftWJMphRPMXRrlmtRgDE|   true|
| 1691453045|ojgTIBUyBHqWPVoICoFr|                null|WHmUjRVQjiQXQNYLEAyF|  false|
| -238201568|CJfSXLMKhTbqisvckqJC|RUvbKYXIZSUMvcNlcRlH|SXyIkpDkLpkpPONORLLW|   true|
| 1131548653|GEBghHaAZIavPVgoGOuW|                null|JkjHjMPHZBuvTUeBAqxk|  false|
| 1054321963|                null|obpMayybaraTlUFZFPgp|BtnCsVPFeqOprbLqWenj|   null|
|-1027916524|glkXDFgFJSerEWYlxfEw|                null|dwQKSSeNdqWACTPACjWN|   true|
|-1533480932|SCyxKTgIBkptytmpQkyD|fkesWhIGMGtixLpocaFg|                null|  false|
|-1031771273|TxxlHYkeEjosIZeHmAPY|                null|MBhKtkpwkUXpchBYnjqT|  false|
| -676987368|                null|BVgpZvExqOklrqUxRuDq|hKNYiPLpPrhuZMQfaAif|   true|
| 2010150091|                null|ChUQAyLXmKHCrCfybwKG|JAgycyZCvQpBjXgmcxsb|  false|
| 2085218491|QdkpvDgeqoVrGwOpafsy|XgsuBqydwxNalNsAFenB|RrtxMVHDOeKdKUDtkewy|  false|
| 1611879159|tBFQfCPUKwDCORyTFyPw|bwBpMUhMPULnOsmkkIMF|BOlecLZTKffYQwAKaxAI|   null|
|       null|XGGBKBrbrbUXuFTtfPRm|MOGlwZfUEcTkBsBsBfud|                null|   true|
|-1628988863|VUWKlEsouDZjtTSHcuaw|trWaSFRiQgbAdvwBORlV|                null|  false|
| -747134015|abjBUFanMPsZQteQhrrX|FwELKOhRVlgvsYgkXAQm|                null|   true|
| 1824948930|uwYOyogeCglIbgEJuMrS|DbUUlZWJDHwgfOGJwofG|vcscAuhMObpLYlKrfgrE|   true|
| 1796758682|uQldLygDSoIXrOErgGqM|YNXTWoSOyQeTaipeOjSs|pFtmILNrXiwQBCttkWmH|  false|
|  249658845|                null|NmnUvndTVnGpSHNcdaYc|aBoGDGTBbvSRGSVoQoTe|   true|
|-1099973878|WrbxVhCXCZdihjReRjmP|dYINjZcFIfcFuYSiGffN|AclpFEVIPXWeYAXpmuqh|   true|
+-----------+--------------------+--------------------+--------------------+-------+

df.count()
res3: Long = 500 

```
