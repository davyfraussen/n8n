package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.modules.{CalcOrganizationAggregates, Module}
import be.openthebox.util._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object ExportOrganizationsCustomColruyt extends Module {

  // Export Colruyt companies with enriched financial history, legal status,
  // Nacebel code, health score, addresses, vestigingen, employees, and publications.
  // Assumes input data from Openthebox standard exports and CalcOrganizationAggregates parquet.

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {

    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export Colruyt companies enriched with financial and legal data")

    // Helper to format addresses from optional address history
    def createFormattedAddress(addressHist: model.AddressHist): Option[String] = {
      val addrOpt: Option[model.Address] = addressHist.addressNlO.orElse(addressHist.addressFrO)
      addrOpt.map { ad =>
        val elems = List(
          Option(ad.number).filter(_.nonEmpty),
          Option(ad.street).filter(_.nonEmpty),
          Option(ad.zipCode).filter(_.nonEmpty),
          Option(ad.city).filter(_.nonEmpty),
          ad.provinceO.filter(_.nonEmpty),
          Option(ad.countryCode).filter(_.nonEmpty)
        )
        elems.flatten.mkString(", ")
      }
    }

    // Load organization aggregates containing main company data
    val orgAggDS: Dataset[model.OrganizationAggregate] = CalcOrganizationAggregates.loadResultsFromParquet

    // Load Openthebox company financials and metadata (pretend loaded from inputArgs or parquet)
    // TODO: Replace with real Openthebox data source and schema
    val opentheboxCompaniesDF = sparkSession.read.option("header", "true").csv(inputArgs.getString("opentheboxCompaniesCsvPath"))

    // Map and select needed columns from Openthebox input, sanitizing and renaming
    val opentheboxCleanedDF = opentheboxCompaniesDF.select(
      $"vatNumber".as("vat"),
      $"turnover_5y_history",
      $"profit_5y_history",
      $"ebitda_5y_history",
      $"eigen_vermogen_5y_history",
      $"founding_date".cast("date").as("foundingDate"),
      $"health_score".cast("double"),
      $"post_address",
      $"vestigingen",
      $"aantal_werknemers".cast("int"),
      $"juridische_situatie",
      $"code22_land_buildings",
      $"nacebel_code",
      $"publications_link"
    )

    // Join organization data with Openthebox financial and metadata on VAT number
    val joinedDF = orgAggDS
      .filter(org => org.vat != null && org.vat.nonEmpty)
      .map(org => (org.vat, org.nameNl, org.nameFr, org.addressHists.lastOption.map(createFormattedAddress)))
      .toDF("vat", "nameNl", "nameFr", "formattedAddressOpt")
      .join(opentheboxCleanedDF, Seq("vat"), "left_outer")
      // Flatten formatted address Option[String] to String, replacing null with empty string
      .withColumn(
        "formattedAddress",
        coalesce(col("formattedAddressOpt"), lit(""))
      )
      .drop("formattedAddressOpt")

    // Select and order final columns for export CSV
    val exportDF = joinedDF.select(
      col("vat"),
      col("nameNl"),
      col("nameFr"),
      col("formattedAddress"),
      col("turnover_5y_history"),
      col("profit_5y_history"),
      col("ebitda_5y_history"),
      col("eigen_vermogen_5y_history"),
      col("foundingDate"),
      col("health_score"),
      col("post_address"),
      col("vestigingen"),
      col("aantal_werknemers"),
      col("juridische_situatie"),
      col("code22_land_buildings"),
      col("nacebel_code"),
      col("publications_link")
    )

    logger.info(s"Exporting Colruyt companies enriched data to CSV file")
    Utils.saveAsCsvFile(exportDF, "ColruytCompaniesEnrichedExport.csv")
  }

}
