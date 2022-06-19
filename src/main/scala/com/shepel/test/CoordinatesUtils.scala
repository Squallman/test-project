package com.shepel.test

import scala.math.{acos, cos, sin, toRadians}

object CoordinatesUtils {

  def calculateDistance(sourceLat: Double, sourceLon: Double, targetLat: Double, targetLon: Double): Double = {
    1000 * 6371 * acos(cos(toRadians(sourceLat)) * cos(toRadians(targetLat)) * cos(toRadians(targetLon) - toRadians(sourceLon)) + sin(toRadians(sourceLat)) * sin(toRadians(targetLat)))
  }

}
