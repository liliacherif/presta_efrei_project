import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, _}
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.Random
import play.api.libs.json.Json

object csvProducer extends App {
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)
  val topic = "presta"
  implicit val messageWrites = Json.writes[Message]

 // val bufferedSource = scala.io.Source.fromFile("/home/brenden/Documents/Efrei/M1/Functionnal_Data_Programming/presta_prj/presta_prj/xaa.csv").getLines().drop(1).foreach { line=>

  val bufferedSource = scala.io.Source.fromFile("/home/brenden/Documents/Efrei/M1/Functionnal_Data_Programming/presta_prj/presta_prj/Parking_Violations_Issued_-_Fiscal_Year_2015.csv").getLines().drop(1).foreach { line =>


    val g_cols = line.split(",").map(_.trim)


    if (g_cols.size >= 35){
    val g_line = s"${g_cols(2)}" + "," + s"${g_cols(4)}" + "," + s"${g_cols(5)}" + "," + s"${g_cols(6)}" + "," + s"${g_cols(7)}" + "," + s"${g_cols(9)}" + "," + s"${g_cols(23)}" + "," + s"${g_cols(33)}" + "," + s"${g_cols(35)}" + ","

    if (g_line.contains(",,") == false) {

      val cols = g_line.split(",").map(_.trim)
      val message = Message(droneid = "-1", state = s"${cols(0)}", date = s"${cols(1)}", violationcode = s"${cols(2)}", vehiculetype = s"${cols(3)}", vehiculebrand = s"${cols(4)}", streetcode = s"${cols(5)}", housenumber = s"${cols(6)}", vehiculecolor = s"${cols(7)}", vehiculeyear = s"${cols(8)}")
      val messageJson: String = Json.toJson(message).toString()
      producer.send(new ProducerRecord[String, String](topic, messageJson))
      //println(messageJson)

    }}
    //println(i)
    //i = i + 1




    }





  println("\nAll sent")
  producer.close()
}


