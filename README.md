# 	一、项目体系结构设计

## 1. 系统架构

业务数据库：采用MongoDB作为数据库

+ 离线推荐部分
  + 离线统计部分：采用 Spark Core + Spark SQL 实现对数据的统计处理
  + 离线统计部分：采用 Spark Core + Spark MLlib 利用 ALS算法实现电影推荐

## 2. 项目数据流程

### 1. 系统初始化部分

通过 Spark SQL 将系统初始化数据加载到 MongoDB 中。

### 2. 离线推荐部分

+ 离线统计：从MongoDB 中加载数据，将`电影平均评分统计`、`电影评分个数统计`、`最近电影评分个数统计`三个统计算法进行运行实现，并将计算结果回写到 MongoDB 中；

+ 离线推荐：从MongoDB 中加载数据，通过 `ALS` 算法分别将【用 户推荐结果矩阵】、【影片相似度矩阵】回写到MongoDB 中；

## 3. 数据模型

### Movie：电影数据表

<img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221005005539.png" alt="image-20201221005005539" style="zoom:50%;" />

### Rating：用户评分表

<img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221005152127.png" alt="image-20201221005152127" style="zoom:50%;" />

### User：用户表

<img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221185208004.png" alt="image-20201221185208004" style="zoom: 50%;" />



# 二、基本环境搭建

>项目主体用 `Scala` 编写，采用 `IDEA 2020.1` 作为开发环境进行项目编写，采用 `maven` 作为项目构建和管理工具。

## 1. 新建项目结构

1. 新建普通maven项目`Movie_Recommendation_System`，删除其src目录，作为父模块管理pom依赖及相关插件打包工具

2. 新建`DataLoad`子模块，用于数据加载
3. 新建`StatisticalRecommendtion`子模块，用于统计推荐
4. 新建`AlsOfflineRecommendation`子模块，实现ALS算法离线推荐

<img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221005901060.png" alt="image-20201221005901060" style="zoom:67%;" />

## 2. pom.xml配置

### 1. 父模块：Movie_Recommendation_System

>我们整个项目需要用到多个工具，它们的不同版本可能会对程序运行造成影响， 所以应该在最外层的 MovieRecommendSystem 中声明所有子项目共用的版本信息

在父模块`MovieRecommandSystem`的pom.xml进行以下设置

+ 配置依赖的版本信息

  ```xml
  <properties>
      <log4j.version>1.2.17</log4j.version>
      <slf4j.version>1.7.22</slf4j.version>
      <mongodb-spark.version>2.0.0</mongodb-spark.version>
      <casbah.version>3.1.1</casbah.version>
      <spark.version>2.1.1</spark.version>
      <scala.version>2.11.8</scala.version>
      <jblas.version>1.2.1</jblas.version>
  </properties>
  ```

+ 配置公共依赖：对于整个项目而言，应该有同样的日志管理

  ```xml
  <dependencies>
      <!--引入共同的日志管理工具-->
      <dependency>
          <groupId>org.slf4j</groupId>
          <artifactId>jcl-over-slf4j</artifactId>
          <version>${slf4j.version}</version>
      </dependency>
      <dependency>
          <groupId>org.slf4j</groupId>
          <artifactId>slf4j-api</artifactId>
          <version>${slf4j.version}</version>
      </dependency>
      <dependency>
          <groupId>org.slf4j</groupId>
          <artifactId>slf4j-log4j12</artifactId>
          <version>${slf4j.version}</version>
      </dependency>
      <dependency>
          <groupId>log4j</groupId>
          <artifactId>log4j</artifactId>
          <version>${log4j.version}</version>
      </dependency>
  </dependencies>
  ```

+ 引入共有插件

  ```xml
  <build>
      <!--声明并引入子项目共有的插件-->
      <plugins>
          <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-compiler-plugin</artifactId>
              <version>3.6.1</version> <!--所有的编译用JDK1.8-->
              <configuration>
                  <source>1.8</source>
                  <target>1.8</target>
              </configuration>
          </plugin>
      </plugins>
      <pluginManagement>
          <plugins>
              <!--maven的打包插件-->
              <plugin>
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-assembly-plugin</artifactId>
                  <version>3.0.0</version>
                  <executions>
                      <execution>
                          <id>make-assembly</id>
                          <phase>package</phase>
                          <goals>
                              <goal>single</goal>
                          </goals>
                      </execution>
                  </executions>
              </plugin>
              <!--该插件用于将scala代码编译成class文件-->
              <plugin>
                  <groupId>net.alchim31.maven</groupId>
                  <artifactId>scala-maven-plugin</artifactId>
                  <version>3.2.2</version>
                  <executions> <!--绑定到maven的编译阶段-->
                      <execution>
                          <goals>
                              <goal>compile</goal>
                              <goal>testCompile</goal>
                          </goals>
                      </execution>
                  </executions>
              </plugin>
          </plugins>
      </pluginManagement>
  </build>
  ```

+ 相关依赖管理

  ```xml
  <dependencyManagement>
      <dependencies>
          <!-- 引入Spark相关的Jar包 -->
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-core_2.11</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-sql_2.11</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-streaming_2.11</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-mllib_2.11</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-graphx_2.11</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <!--scala-->
          <dependency>
              <groupId>org.scala-lang</groupId>
              <artifactId>scala-library</artifactId>
              <version>${scala.version}</version>
          </dependency>
      </dependencies>
  </dependencyManagement>
  ```

### 3. 子模块：DataLoad

> 对于具体的 DataLoad 子项目，需要 spark 相关组件，还需要 mongodb、elastic
> search 的相关依赖，我们在 pom.xml 文件中引入所有依赖（在父项目中已声明的不 需要再加详细信息）

```xml
<dependencies>
    <!-- Spark的依赖引入 -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
    </dependency>
    <!-- 引入Scala -->
    <dependency>
        <groupId>org.scala-lang</groupId>
        <artifactId>scala-library</artifactId>
    </dependency>
    <!-- MongoDB的驱动 -->
    <dependency>
        <groupId>org.mongodb</groupId>
        <artifactId>casbah-core_2.11</artifactId>
        <version>${casbah.version}</version>
    </dependency>
    <dependency>
        <groupId>org.mongodb.spark</groupId>
        <artifactId>mongo-spark-connector_2.11</artifactId>
        <version>${mongodb-spark.version}</version>
    </dependency>
</dependencies>
```

### 4. 子模块：StatisticalRecommendtion

```xml
<dependencies>
    <!-- Spark的依赖引入 -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
    </dependency>
    <!-- 引入Scala -->
    <dependency>
        <groupId>org.scala-lang</groupId>
        <artifactId>scala-library</artifactId>
    </dependency>
    <!-- MongoDB的驱动 -->
    <dependency>
        <groupId>org.mongodb</groupId>
        <artifactId>casbah-core_2.11</artifactId>
        <version>${casbah.version}</version>
    </dependency>
    <!-- 用于Spark和MongoDB的对接 -->
    <dependency>
        <groupId>org.mongodb.spark</groupId>
        <artifactId>mongo-spark-connector_2.11</artifactId>
        <version>${mongodb-spark.version}</version>
    </dependency>
</dependencies>
```

### 5. 子模块：AlsOfflineRecommendation

```xml
<dependencies>
    <dependency>
        <groupId>org.scalanlp</groupId>
        <artifactId>jblas</artifactId>
        <version>${jblas.version}</version>
    </dependency>
    <!-- Spark的依赖引入 -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
    </dependency>
    <!--机器学习-->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-mllib_2.11</artifactId>
    </dependency>
    <!-- 引入Scala -->
    <dependency>
        <groupId>org.scala-lang</groupId>
        <artifactId>scala-library</artifactId>
    </dependency>
    <!-- MongoDB的驱动 -->
    <dependency>
        <groupId>org.mongodb</groupId>
        <artifactId>casbah-core_2.11</artifactId>
        <version>${casbah.version}</version>
    </dependency>
    <!-- 用于Spark和MongoDB的对接 -->
    <dependency>
        <groupId>org.mongodb.spark</groupId>
        <artifactId>mongo-spark-connector_2.11</artifactId>
        <version>${mongodb-spark.version}</version>
    </dependency>
</dependencies>
```



# 三、数据加载模块

该模块用于将原始的`.csv`数据文件，通过`SparkContext`的`textFile`方法读取出来并转换为`DataFrame`，再利用 Spark SQL 提供的 write 方法进行数据的分布式插入到`MongoDB`数据库中

## 1. 资源准备

> 将数据文件 movies.csv，ratings.csv，tags.csv 复制到资源文件目录 src/main/resources 下

![image-20201221010738327](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221010738327.png)

> 然后编写`log4j`配置文件（log4j 对日志的管理，需要通过配置文件来生效）

在src/main/resources 下新建配置文件 `log4j.properties`

```properties
log4j.rootLogger=info, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS}  %5p --- [%50t]  %-80c(line:%5L)  :  %m%n
```

![image-20201219191503158](https://gitee.com/zhong_siru/images/raw/master//img/image-20201219191503158.png)

## 2. 编写代码

讲`java`目录改名为`scala`，然后新建`DataLoader.scala`

```scala
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Movie 数据集
 *
 * 260                                         电影ID，mid
 * Star Wars: Episode IV - A New Hope (1977)   电影名称，name
 * Princess Leia is captured and held hostage  详情描述，descri
 * 121 minutes                                 时长，timelong
 * September 21, 2004                          发行时间，issue
 * 1977                                        拍摄时间，shoot
 * English                                     语言，language
 * Action|Adventure|Sci-Fi                     类型，genres
 * Mark Hamill|Harrison Ford|Carrie Fisher     演员表，actors
 * George Lucas                                导演，directors
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
 * Rating数据集
 *
 * 1,31,2.5,1260759144
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
 * Tag数据集
 *
 * 15,1955,dentist,1193435061
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)
	
/**
 * 把mongo的配置封装成样例类
 *
 * @param uri MongoDB连接
 * @param db  MongoDB数据库
 */
case class MongoConfig(uri: String, db: String)

object DataLoad {

  // 定义常量
  val MOVIE_DATA_PATH = "D:\\学习\\IDEA project\\Movie_Recommendation_System\\DataLoad\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\学习\\IDEA project\\Movie_Recommendation_System\\DataLoad\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\学习\\IDEA project\\Movie_Recommendation_System\\DataLoad\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoad")

    // 创建一个SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    // 加载数据
    val movieRDD = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)

    val movieDF = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
      }
    ).toDF()

    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)

    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)
    //将tagRDD装换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 将数据保存到MongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // 数据预处理，把movie对应的tag信息添加进去，加一列 tag1|tag2|tag3...
    import org.apache.spark.sql.functions._

    /**
     * mid, tags
     * tags: tag1|tag2|tag3...
     */
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    // newTag和movie做join，数据合并在一起，左外连接
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    sparkSession.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个mongodb的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如果mongodb中已经有相应的数据库，先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 将DF数据写入对应的mongodb表中
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }
}
```

## 3. 测试

首先开启MongoDB服务，运行该方法进行测试

+ 查看所有数据表，可以看到`recommender`数据库成功写入

  ```sql
  show dbs
  ```

![image-20201221112629455](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221112629455.png)

+ 切换到`recommender`数据库，查看所有的表，可以发现三张表成功写入

  ```sql
  show tables
  ```

![image-20201221112827677](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221112827677.png)

+ 检验写入的数据是否正确，与对应的`.csv`文件对比，三个表的数据完全正确

  ```sql
  db.Movie.find().pretty()	#查看电影表数据
  db.Movie.find().count()		#查看电影表元组个数
  ```

![image-20201221112959957](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221112959957.png)

![image-20201221113318426](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221113318426.png)

![image-20201221113406814](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221113406814.png)



# 四、基于统计的推荐

这是一种离线推荐服务，就是综合用户的所有历史数据，采用统计算法来做出推荐，这里分为三种：

+ 历史热门电影推荐：根据数据计算历史评分次数最多的电影
+ 最近热门电影统计：按月为单位计算最近时间的月份里面评分数最多的电影集合
+ 电影平均得分统计：根据历史数据中所有用户对电影的评分，周期性的计算每个电影的平均得分
+ 类别top10电影统计：根据提供的所有电影类别，分别计算每种类型的电影集合中评分最高的 10 个电影

## 1. 资源准备

在src/main/resources 下新建配置文件 `log4j.properties`

```properties
log4j.rootLogger=info, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS}  %5p --- [%50t]  %-80c(line:%5L)  :  %m%n
```

## 2. 编写代码

讲`java`目录改名为`scala`，然后新建`StatisticsRecommend.scala`

```java
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
```

## 3. 测试

运行程序，等待程序运行完成，查看MongoDB数据库`recommender`中的所有表

可以发现对应四种统计生成了新的四张表：

<img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221193412007.png" alt="image-20201221193412007" style="zoom:67%;" />

+ 历史热门电影推荐：`RateMoreMovies`

  <img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221193031973.png" alt="image-20201221193031973" style="zoom: 50%;" />

+ 最近热门电影统计：`RateMoreMoviesRecently`

  <img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221193018351.png" alt="image-20201221193018351" style="zoom:50%;" />

+ 电影平均得分统计：`AverageMoviesScore`

  <img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221193000721.png" alt="image-20201221193000721" style="zoom:50%;" />

+ 类别top10电影统计：`GenersTopMoives`

  <img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221193301771.png" alt="image-20201221193301771" style="zoom:50%;" />

我们可以查看具体表内的信息

```shell
db.AverageMovies.find().pretty()			#查看AverageMovies表
db.RateMoreMovies.find().pretty()			#查看RateMoreMovies表
db.RateMoreRecentlyMovies.find().pretty()	 #查看RateMoreRecentlyMovies表
db.GenresTopMoives.find().pretty()	 		#查看GenersTopMoivess表
```

历史热门电影推荐：

![image-20201221185809838](C:\Users\zsr204\AppData\Roaming\Typora\typora-user-images\image-20201221185809838.png)

最近热门电影统计：

![image-20201221185837223](C:\Users\zsr204\AppData\Roaming\Typora\typora-user-images\image-20201221185837223.png)

电影平均得分统计：

![image-20201221185715257](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221185715257.png)

类别top10电影统计：

![image-20201221194025492](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221194025492.png)



# 五、基于隐语义模型的协同过滤推荐

> 项目采用 ALS 作为协同过滤算法，分别根据 MongoDB 中的用户评分表和电影 数据集计算用户电影推荐矩阵

通过 ALS 训练出来的 Model 来计算所有当前用户电影的推荐矩阵，主要思路如 下：

1. UserId 和 MovieID 做笛卡尔积，产生（uid，mid）的元组 
2. 通过模型预测（uid，mid）的元组。 
3. 将预测结果通过预测分值进行排序。 
4. 返回分值最大的 K 个电影，作为当前用户的推荐。

## 1. 资源准备

在src/main/resources 下新建配置文件 `log4j.properties`

```properties
log4j.rootLogger=info, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS}  %5p --- [%50t]  %-80c(line:%5L)  :  %m%n
```

## 2. 编写ALS推荐代码

首先编写`AlsOfflineRecommend.scala`

```java
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

//基于评分数据的隐语义模型，只需要评分数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

//定义基准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义基于用预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//基于隐语义模型电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recd: Seq[Recommendation])

object AlsOfflineRecommend {
  //定义表名和常量
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // 创建一个SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据
    //从mongodb中加载数据
    val ratingRDD = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) //转换成rdd，并且去掉时间戳
      .cache()

    //从rating数据中提取所有uid和mid去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    //训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (50, 5, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    //基于用户和电影的隐特征，计算预测评分，得到用户的推荐列表
    //计算user和movie的笛卡尔积，得到空评分矩阵
    val userMovies = userRDD.cartesian(movieRDD);
    //调用model的predict方法预测评分
    val preRatings = model.predict(userMovies)
    val userRecs = preRatings
      .filter(_.rating > 0) //过滤出评分大于0的项
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.stop()
  }
}
```

## 3. 模型评估和参数选取代码

>在上述模型训练的过程中，我们直接给定了隐语义模型的 rank,iterations,lambda三个参数。
>
>对于我们的模型，这并不一定是最优的参数选取，所以我们需要对模型 进行评估。通常的做法是计算均方根误差（RMSE），考察预测评分与实际评分之 间的误差。
>
>![image-20201221194959338](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221194959338.png)有了 RMSE，我们可以就可以通过多次调整参数值，来选取 RMSE 最小的一组 作为我们模型的优化选择。

编写训练方法`ALSTrainer.scala`

```java
import AlsOfflineRecommend.MONGODB_RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")

    // 创建一个SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载评分数据
    val ratingRDD = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) //转换成rdd，并且去掉时间戳
      .cache()

    //随机切分数据集，生成训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    //模型参数选择，输出最优选择
    adjustALSParam(trainingRDD, testRDD)

    sparkSession.close()
  }

  //输出最终的最优参数
  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    //这里指定迭代次数为5，rank和lambda在几个值中选取调整
    val result = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        //计算当前参数对应模型的rmse，返回Double
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    //控制台打印输出最优参数
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val userMovies = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userMovies)
    val real = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    //计算RMSE
    sqrt(
      real.join(predict).map { case ((uid, mid), (real, pre)) =>
        // ]真实值和预测值之间的差
        val err = real - pre
        err * err
      }.mean()
    )
  }
}
```

## 4. 测试

### 1. 测试ALS推荐

运行`AlsOfflineRecommend.scala`，可以发现新生成了表`userRecs`

![image-20201221200408149](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221200408149.png)

这是基于ALS算法推荐的电影列表

<img src="https://gitee.com/zhong_siru/images/raw/master//img/image-20201221200304364.png" alt="image-20201221200304364" style="zoom:67%;" />

我们可以查看该表的详细信息

```shell
db.UserRecs.find().pretty()
```

![image-20201221200502751](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221200502751.png)

可以看到推荐的电影列表信息

### 2. 模型参数评估

运行`ALSTrainer.java`，可以看到从我们设置的参数中打印的最优参数

![image-20201221202251519](https://gitee.com/zhong_siru/images/raw/master//img/image-20201221202251519.png)

