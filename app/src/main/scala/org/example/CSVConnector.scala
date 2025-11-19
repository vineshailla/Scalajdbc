package org.example
import java.sql.{Connection, DriverManager, PreparedStatement}
import com.github.tototoshi.csv._
import scala.util.{Try, Using}

object CSVConnector {

  // Database configuration - UPDATE THIS URL FOR YOUR SQL SERVER
  val url = "jdbc:sqlserver://DESKTOP-TFOF660:1433;databaseName=vinnu;encrypt=false"
  val userName = "sa"
  val password = "vinesh"


  def main(args: Array[String]): Unit = {
    val csvFilePath = "C:\\csvfiles\\customers-100.csv"

    println("Starting CSV to SQL Server import...")

    // Read and parse CSV
    val customers = readCSVFile(csvFilePath)

    if (customers.nonEmpty) {
      println(s"✓ Read ${customers.size} records from CSV")

      // Create table (if not exists)
      createCustomersTable()

      // Insert data into database
      insertCustomers(customers)
    } else {
      println("✗ No records found in CSV file")
    }
  }

  case class Customer(
                       index: Long,
                       customerId: String,
                       firstName: String,
                       lastName: String,
                       company: String,
                       city: String,
                       country: String,
                       phone1: String,
                       phone2: String,
                       email: String,
                       subscriptionDate: String,
                       website: String
                     )

  def readCSVFile(filePath: String): List[Customer] = {
    println(s"Reading CSV file: $filePath")

    Try {
      val reader = CSVReader.open(new java.io.File(filePath))
      val records = reader.allWithHeaders()
      reader.close()

      records.map { row =>
        Customer(
          index = Try(row("Index").toLong).getOrElse(0L),
          customerId = row.getOrElse("Customer Id", ""),
          firstName = row.getOrElse("First Name", ""),
          lastName = row.getOrElse("Last Name", ""),
          company = row.getOrElse("Company", ""),
          city = row.getOrElse("City", ""),
          country = row.getOrElse("Country", ""),
          phone1 = row.getOrElse("Phone 1", ""),
          phone2 = row.getOrElse("Phone 2", ""),
          email = row.getOrElse("Email", ""),
          subscriptionDate = row.getOrElse("Subscription Date", ""),
          website = row.getOrElse("Website", "")
        )
      }
    }.recover {
      case e: Exception =>
        println(s"Error reading CSV file: ${e.getMessage}")
        e.printStackTrace()
        List.empty[Customer]
    }.get
  }

  def createCustomersTable(): Unit = {
    Using.resource(getConnection()) { connection =>
      val createTableSQL = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
        CREATE TABLE customers (
          id BIGINT IDENTITY(1,1) PRIMARY KEY,
          [index] BIGINT,
          customer_id NVARCHAR(100),
          first_name NVARCHAR(100),
          last_name NVARCHAR(100),
          company NVARCHAR(255),
          city NVARCHAR(100),
          country NVARCHAR(100),
          phone_1 NVARCHAR(50),
          phone_2 NVARCHAR(50),
          email NVARCHAR(255),
          subscription_date NVARCHAR(50),
          website NVARCHAR(500),
          import_timestamp DATETIME DEFAULT GETDATE()
        )
      """

      val statement = connection.createStatement()
      statement.execute(createTableSQL)
      println("✓ Customers table created/verified successfully")
    }
  }

  def insertCustomers(customers: List[Customer]): Unit = {
    Using.resource(getConnection()) { connection =>
      connection.setAutoCommit(false)

      val insertSQL = """
        INSERT INTO customers (
          [index], customer_id, first_name, last_name, company, city, country,
          phone_1, phone_2, email, subscription_date, website
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """

      Using.resource(connection.prepareStatement(insertSQL)) { preparedStatement =>
        var successCount = 0
        var errorCount = 0

        customers.foreach { customer =>
          try {
            preparedStatement.setLong(1, customer.index)
            preparedStatement.setString(2, customer.customerId)
            preparedStatement.setString(3, customer.firstName)
            preparedStatement.setString(4, customer.lastName)
            preparedStatement.setString(5, customer.company)
            preparedStatement.setString(6, customer.city)
            preparedStatement.setString(7, customer.country)
            preparedStatement.setString(8, customer.phone1)
            preparedStatement.setString(9, customer.phone2)
            preparedStatement.setString(10, customer.email)
            preparedStatement.setString(11, customer.subscriptionDate)
            preparedStatement.setString(12, customer.website)

            preparedStatement.addBatch()
            successCount += 1

            // Execute batch every 50 records to avoid memory issues
            if (successCount % 50 == 0) {
              val batchResults = preparedStatement.executeBatch()
              println(s"Processed $successCount records...")
            }

          } catch {
            case e: Exception =>
              println(s"Failed to prepare customer ${customer.customerId}: ${e.getMessage}")
              errorCount += 1
          }
        }

        // Execute remaining batches
        try {
          val batchResults = preparedStatement.executeBatch()
          connection.commit()
          println(s" Successfully inserted $successCount records into database!")
          if (errorCount > 0) {
            println(s" Failed to insert $errorCount records due to errors")
          }
        } catch {
          case e: Exception =>
            connection.rollback()
            println(s" Batch insert failed: ${e.getMessage}")
        }
      }
    }
  }

  def getConnection(): Connection = {
    DriverManager.getConnection(url , userName , password)
  }
}