package org.ruivieira.ebert

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.mongodb.scala.{MongoClient, _}

class Model(val rank : Int, val userFeatures: RDD[(Int, Array[Double])], val productFeatures: RDD[(Int, Array[Double])]) {

}

object Main {

  // connect to the MongoDB

  val mongoClient = MongoClient()

  val modelsDB: MongoDatabase = mongoClient.getDatabase("models")

  val models: MongoCollection[Document] = modelsDB.getCollection("models")


  def loadLinks(sc : SparkContext,
                path: String) : RDD[(String, String)] = {
    sc.textFile(path)
      .filter(x => x != "movieId,imdbId,tmdbId")
      .map(x => x.split(",")).map(x => (x(0), x(1))).cache()
  }

  def loadMovies(sc : SparkContext,
                path: String) : RDD[(String, String)] = {
    sc.textFile(path)
      .filter(x => x != "movieId,title,genres")
      .map(x => x.split(",")).map(x => (x(0), x(1))).cache()
  }

  def loadRatings(sc : SparkContext,
                 path: String) : RDD[(String, String, String)] = {
    sc.textFile(path)
      .filter(x => x != "userId,movieId,rating,timestamp")
      .map(x => x.split(",")).map(x => (x(0), x(1), x(2))).cache()
  }

  def loadTags(sc : SparkContext,
                  path: String) : RDD[(String, String, String)] = {
    sc.textFile(path)
      .filter(x => x != "userId,movieId,tag,timestamp")
      .map(x => x.split(",")).map(x => (x(0), x(1), x(2))).cache()
  }

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val model = ModelReader.readModel(sc, models, 1).get

    val matrix = new MatrixFactorizationModel(2, model.userFeatures, model.productFeatures)

    println(s"Recommendation for user 100 of product 200 is ${matrix.predict(100, 200)}")

    Thread.sleep(2000)

  }


}
