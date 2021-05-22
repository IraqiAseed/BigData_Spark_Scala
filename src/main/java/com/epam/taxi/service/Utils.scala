package com.epam.taxi.service

import com.epam.taxi.model.{Driver, Trip}
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object Utils {

  def getAmountOfTripsToBostonLongerThanTenKm(trips: RDD[Trip]): Long = {
    trips.filter(trip => trip.location.equalsIgnoreCase("boston"))
      .filter(trip => trip.km > 10).count
  }

  def getSumOfAllKmTripsToBoston(trips: RDD[Trip]): Long = {
    trips.filter(trip => trip.location.equalsIgnoreCase("boston"))
      .map(trip => trip.km).reduce(Integer.sum)
  }

  private def getPairsTripIdAndKm(trips: RDD[Trip]): RDD[(Long, Int)] = {
    trips.map(trip => Tuple2(trip.driverId, trip.km)).reduceByKey(Integer.sum)
  }

  private def getPairsDriverIdAndName(drivers: RDD[Driver]): RDD[(Long, String)] = {
    drivers.map(driver => Tuple2(driver.id, driver.name))
  }

  def getThreeDriversWithMaxTotalKm(trips: RDD[Trip], drivers: RDD[Driver]): List[String] = {
    getPairsTripIdAndKm(trips)
      .join(getPairsDriverIdAndName(drivers))
      .map(tuple => Tuple2(tuple._2._1, tuple._2._2))
      .sortByKey(false)
      .map(tuple => tuple._2)
      .take(3)
      .toList
  }

  def performanceInfo(trips: RDD[Trip]): Unit = {

    val tripsLessThanFiveKm: Long = trips.filter(trip => trip.km < 5).count
    println(s"trips less than five km: $tripsLessThanFiveKm")

    val tripsBetweenFiveAndTenKmBothIncluded: Long = trips.filter(trip => trip.km >= 5).filter(trip => trip.km <= 10).count
    println(s"trips between five and ten km both included: $tripsBetweenFiveAndTenKmBothIncluded")

    val tripsGreaterThanTenKm: Long = trips.filter(trip => trip.km > 10).count
    println(s"trips greater than 10 km: $tripsGreaterThanTenKm")

    val tripsMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]
    tripsMap.put("LessThanFiveKm", tripsLessThanFiveKm)
    tripsMap.put("tripsBetweenFiveAndTenKmBothIncluded", tripsBetweenFiveAndTenKmBothIncluded)
    tripsMap.put("tripsGreaterThanTenKm", tripsGreaterThanTenKm)

    val popular = tripsMap.maxBy(_._2)
    val mostPopular = tripsMap.seq.filter(element => element._2 == popular._2)
    println(mostPopular)

  }


}
