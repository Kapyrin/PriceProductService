package simulations

import io.gatling.commons.validation.Success
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.util.Random

class PriceUpdateSimulation extends Simulation {
  val httpProtocol = http
    .baseUrl("http://localhost:8080")
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  val scn = scenario("Price Updates Load Test")
    .exec(session => {
      val batchSize = Random.nextInt(10) + 1

      val updates = (1 to batchSize).map { _ =>
        val isValid = Random.nextDouble() > 0.01
        val id = if (isValid) Random.nextInt(100000) + 1 else -(Random.nextInt(100000) + 1)
        val name = s"Производитель_${Random.alphanumeric.take(5).mkString}"
        val price = Math.round((Random.nextDouble() * 400 + 100) * 100) / 100.0
        (id, name, price, isValid)
      }

      val jsonArray = updates.map { case (id, name, price, _) =>
        s"""{ "product_id": $id, "manufacturer_name": "$name", "price": $price }"""
      }.mkString("[", ",", "]")

      val validIdToCheck = updates.find(_._4).map(_._1)
      val sessionWithJson = session.set("jsonArray", jsonArray)
      val finalSession = validIdToCheck.map(id => sessionWithJson.set("validId", id)).getOrElse(sessionWithJson)
      
      Success(finalSession)
    })
    .exec(http("POST /price-updates")
      .post("/price-updates")
      .body(StringBody("${jsonArray}")).asJson
      .check(status.is(202)))
    .pause(1.second)
    .doIf(session => session.contains("validId")) {
      exec(http("GET /average-price/${validId}")
        .get("/average-price/${validId}")
        .check(status.is(200)))
    }

  setUp(
    scn.inject(
      constantUsersPerSec(500).during(20.seconds)
    )
  ).protocols(httpProtocol)
}