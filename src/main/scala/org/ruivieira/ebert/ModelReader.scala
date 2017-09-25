package org.ruivieira.ebert

import org.apache.spark.SparkContext
import org.mongodb.scala.{Document, MongoCollection}
import org.mongodb.scala.model.Filters.equal

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ModelReader {

  private def parseFeatures(
      doc: Document,
      featureType: String): IndexedSeq[(Int, Array[Double])] = {

    val featureArray = doc.get(featureType).toList.head.asArray()

    // TODO: do not hard-code rank

    (0 until featureArray.size()).map { n =>
      val feature = featureArray.get(n)

      val bsonFeatures = feature.asDocument().get("features").asArray()

      val values = Array(bsonFeatures.get(0).asDouble().doubleValue(),
                         bsonFeatures.get(1).asDouble().doubleValue())

      (feature.asDocument().get("id").asInt32().getValue, values)

    }
  }

  def readModel(sc: SparkContext,
                collection: MongoCollection[Document],
                id: Int): Option[Model] = {
    val parser = collection.find(equal("id", id))

    val result = Await.ready(parser.toFuture, Duration.Inf)

    Try(Await.result(parser.toFuture(), Duration.Inf)) match {
      case Success(docs) => {

        val doc = docs(0)

        val userFeaturesRDD = sc.parallelize(parseFeatures(doc, "userFeatures"))

        val productFeaturesRDD =
          sc.parallelize(parseFeatures(doc, "productFeatures"))

        Some(new Model(2, userFeaturesRDD, productFeaturesRDD))

      }
      case Failure(_) => { None }
      case _          => { None }
    }

  }

}
