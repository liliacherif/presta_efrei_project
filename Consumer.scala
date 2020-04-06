import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.spark.streaming.dstream.InputDStream


object Consumer extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._



  val topic = Array("presta")


  val mail: Mail = new Mail()
  //val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092","bootstrap.servers" -> "localhost:9092", "auto.offset.reset" -> "smallest")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream_id",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val kafkaStreams:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topic, kafkaParams)
  )


  kafkaStreams.map(record => (record.value)).foreachRDD( rdd => {
    if(!rdd.isEmpty())
    {
      rdd.saveAsTextFile("hdfs://localhost:9000/data/prestacop")


      //rdd.saveAsTextFile("/home/brenden/Documents/Efrei/M1/Functionnal_Data_Programming/presta_prj/presta_prj/src/main/Hadoop")
      rdd.map(x => {
        val split = x.split(',')
        //println(split(3))
        if(split(3).toString == "\"violationcode\":\"alert\"") {

          val mess = "NEW ALERT : " + split.mkString(",")
          mail.send(mess, "ALERT")


          }
      }).foreach(print)


    }
  })

  ssc.start()
  ssc.awaitTermination()

}