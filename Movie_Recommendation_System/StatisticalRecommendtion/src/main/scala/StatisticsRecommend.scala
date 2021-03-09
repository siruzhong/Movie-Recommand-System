import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

//定义基准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义电影类别top10样例类
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])


object StatisticsRecommend {
  //定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies" //历史热门统计
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies" //近期热门统计
  val AVERAGE_MOVIES = "AverageMovies" //电影平均得分统计
  val GENRES_TOP_MOVIES = "GenresTopMovies" //每个类别优质电影统计

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommend")

    // 创建一个SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //从mongodb中加载数据
    val ratingDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建ratings视图
    ratingDF.createOrReplaceTempView("ratings")

    //1.历史热门统计,历史评分最多
    val rateMoreMoviesDF = sparkSession.sql("select mid,count(mid) as count from ratings group by mid")
    //把结果写入对应的mongodb表中
    storeInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    //2.近期热门统计,按照''yyyyMM''格式选取最近的评分数据,统计评分个数
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM");
    //注册udf，把时间戳转换成年月格式
    sparkSession.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    //对原始数据做预处理,去掉uid
    val ratingOfYearMonth = sparkSession.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingsOfMonthYear")
    //从ratingsOfMonthYear中查找各个电影在各个月份的评分，mid，count，yearmonth
    val rateMoreRecentlyMoviesDF = sparkSession.sql("select mid, count(mid) as count, yearmonth from ratingsOfMonthYear group by yearmonth,mid order by yearmonth desc, count desc")
    //存入mongodb
    storeInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES);

    //3.优质电影统计,统计电影的平均评分
    val averageMoviesDF = sparkSession.sql("select mid, avg(score) as avg from ratings group by mid")
    storeInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    //4.各类别电影top统计
    //定义所有类别
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Famil y", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")
    //把平均评分j加入movie列表里，加一列，inner join
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))
    //为做笛卡尔积，将genres转换为rdd
    val genresRDD = sparkSession.sparkContext.makeRDD(genres)
    //计算类别top10，首先对类别和电影做笛卡尔积
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        //条件过滤：找出movie的字段genres包含当前类别的那些
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map {
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }
      .toDF()
    storeInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)
  }

  def storeInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}