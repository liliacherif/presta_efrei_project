import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, _}
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.Random
import play.api.libs.json.Json
object Producer extends App {
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)
  val topic = "presta"
  implicit val messageWrites = Json.writes[Message]
  def simulatorMessage(counter: Int): Unit = {

    //DRONE SIMULATOR
    val model = List("SUBN", "VAN", "PICK", "TANK", "MINI", "TAXI")
    val brand = List("LEXUS", "HONDA", "NISSAN", "BMW", "ROVER", "ROLLS", "FORD")
    val color = List("BROWN", "WHITE", "BLACK", "BLEU", "SILVER", "GREY", "RED")
    val vmodel = model(Random.nextInt(model.length))
    val vbrand = brand(Random.nextInt(brand.length))
    val vcolor = color(Random.nextInt(color.length))
    //generate random date
    val day = 1 + Random.nextInt(29)
    val month = 1 + Random.nextInt(11)
    val year = 2015 + Random.nextInt(5)
    val date = s"$day/$month/$year"
    //Message
    val alert = ((counter % 20) + 20) % 20
    val m ={
      if(alert==0) Message(Random.nextInt(1000).toString, "NY", date, s"alert", vmodel, vbrand, Random.nextInt(2000).toString, Random.nextInt(300).toString, vcolor, (1990 +  Random.nextInt(30)).toString)
      else Message(Random.nextInt(1000).toString, "NY", date, Random.nextInt(100).toString, vmodel, vbrand, Random.nextInt(2000).toString, Random.nextInt(300).toString, vcolor, (1990 +  Random.nextInt(30)).toString)
    }
    val value: String = Json.toJson(m).toString()
    if (counter != 0)
    {
      producer.send(new ProducerRecord[String, String](topic, "key", value))
      val count = counter-1
      simulatorMessage(count)
    }
  }
  //DRONE SIMULATOR
  simulatorMessage(50)

  println("\nAll sent")
  producer.close()
}


