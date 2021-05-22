package com.epam.taxi

import com.epam.taxi.model.{Driver, Trip}
import com.epam.taxi.service.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.lang.Long.parseLong

object Manager {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val sparkConf: SparkConf = new SparkConf().setAppName("Taxi - Spark Scala RDD").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val driversLines: RDD[String] = sc.textFile("data/taxi/drivers.txt")
    val tripsLines: RDD[String] = sc.textFile("data/taxi/trips.txt")

    val numberOfDrivers: Long = driversLines.count
    println(s"number of drivers: $numberOfDrivers")

    val numberOfTrips: Long = tripsLines.count
    println(s"number of trips: $numberOfTrips")

    val trips: RDD[Trip] =
      tripsLines.map((line: String) => line.split(" "))
        .map((arg: Array[String]) => Trip(parseLong(arg(0).trim), arg(1).trim, arg(2).trim.toInt))

    val drivers: RDD[Driver] =
      driversLines.map((line: String) => line.split(","))
        .map((arg: Array[String]) => Driver(parseLong(arg(0).trim), arg(1).trim, arg(2).trim, arg(3).trim))

    val amountOfTripsToBostonLongerThanTenKm: Long = Utils.getAmountOfTripsToBostonLongerThanTenKm(trips)
    println(s"amount of trips to Boston longer than 10 km: $amountOfTripsToBostonLongerThanTenKm")

    val sumOfAllKmTripsToBoston: Long = Utils.getSumOfAllKmTripsToBoston(trips)
    println(s"sum of all km trips to Boston: $sumOfAllKmTripsToBoston")

    val ThreeDriversWithMaxTotalKm: List[String] = Utils.getThreeDriversWithMaxTotalKm(trips, drivers)
    println(s"The Three Drivers with max total km: $ThreeDriversWithMaxTotalKm.")

    //Performance lab
    Utils.performanceInfo(trips)

  }

}
