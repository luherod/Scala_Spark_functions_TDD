# Scala and Spak functions with TDD

## About this project

This project showcases the use of Functional Programming in Scala with Apache Spark, implementing transformations on DataFrames and RDDs while following Test-Driven Development (TDD) principles. It includes multiple functions for data manipulation and their respective tests to ensure correctness.  

Key features include pure functions, immutability, and high-order functions, leveraging Spark SQL and DataFrame APIs for efficient data processing.

## Recommended Tools  
- **IDE:** IntelliJ IDEA

## Requirements:


The complete project is uploaded to GitHub, with all requirements already configured. However, if rebuilding the project from scratch is desired, these are the configurations to consider:

* **JDK:** Amazon Corretto-18.0.2

* **Scala:** 2.12.15

* Include the following library depencies in [build.sbt](https://github.com/luherod/Scala_Spark_functions_TDD/blob/main/Scala_Spark_fucntions_TDD/build.sbt) and install them:
    
    ```
    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "3.2.4",
        "org.apache.spark" %% "spark-sql" % "3.2.4",
        "org.scalatest" %% "scalatest" % "3.0.8" % Test
    )
    ```

* Configure the **VM options** for the Test file as follows:

    ```
    --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED
    ```



## Files description:

* [functions.scala](https://github.com/luherod/Scala_Spark_functions_TDD/blob/main/Scala_Spark_fucntions_TDD/src/main/scala/functions_package/functions.scala): Spark-based functions for data processing.

* [functionsTest.scala](https://github.com/luherod/Scala_Spark_functions_TDD/blob/main/Scala_Spark_fucntions_TDD/src/test/scala/functions_package/functionsTest.scala): tests for the functions in functions.scala, verifying their correctness.

* [TestInit.scala](https://github.com/luherod/Scala_Spark_functions_TDD/blob/main/Scala_Spark_fucntions_TDD/src/test/scala/utils/TestInit.scala): utility functions and setup for Spark-based testing.

* [ventas.csv](https://github.com/luherod/Scala_Spark_functions_TDD/blob/main/Scala_Spark_fucntions_TDD/src/test/resources/ventas.csv): CSV file containing sample data to be imported for executing one of the tests.

## Author

Lucía Herrero Rodero.
