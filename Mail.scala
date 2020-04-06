


import java.util.Properties

import javax.mail.{Authenticator, Message, PasswordAuthentication, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.Message.RecipientType

import scala.io.Source


class Mail {
  val host = "smtp.gmail.com"
  val port = "587"

  val address = "brenden.balane@efrei.net"
  val username = "prestacopprj@gmail.com"
  val password = "prestacop2020"

  def send(text:String, subject:String) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, new GmailAuthenticator("prestacopprj@gmail.com", "prestacop2020"))
    val message = new MimeMessage(session)
    message.addRecipient(RecipientType.TO, new InternetAddress(address));
    message.setSubject(subject)
    message.setContent(text, "text/html")

    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
  }






}

class GmailAuthenticator(username: String, password: String) extends Authenticator {
  override def getPasswordAuthentication(): PasswordAuthentication = new PasswordAuthentication(username, password)
}