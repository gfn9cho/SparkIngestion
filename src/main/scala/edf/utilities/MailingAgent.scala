package edf.utilities

import java.util.Properties

import javax.mail.Message.RecipientType
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{MessagingException, Session, Transport}



/** Mail constructor.
 *
  *  @constructor Mail
  *  @param host Host
  */
object MailingAgent {
  //val fromEmailAddress = "EDF Process"
  //val emailSubject = "Notification from ED Framework"
  //val toEmailAddress = "raghavendra.chandrappa@stateauto.com"
  //val ccEmailAddress = "raghavendra.chandrappa@stateauto.com"
  val emailFooter = "\n\nThis is system generated notification, please don't reply to this email."
  val session = Session.getDefaultInstance(new Properties() {
    put("mail.smtp.host", "smtprelay.corp.stateauto.com")

  })

  /** Send email message.
    *
    * @param from    From
    * @param tos     Recipients
    * @param ccs     CC Recipients
    * @param subject Subject
    * @param text    Text
    * @throws MessagingException
    */

  // This is the method for sending the email by passing required parameters .
  def sendMail(from: String = "raghavendra.chandrappa@stateauto.com",
               tos: String = "raghavendra.chandrappa@stateauto.com",
               ccs: String = "",
               subject: String = "Notification from ED Framework",
               text: String) {
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(from))
    var emailBody = text+emailFooter;
  // Validating TO email string and all the email addresses passed
    validString(tos) match {
      case Some(toEmailString) => {
        val toList = toEmailString.split(",", -1).toList
        for (to <- toList) {
            validateEmailAddress(to) match {
            case Some(toEmail) =>
              message.addRecipient(RecipientType.TO, new InternetAddress(toEmail))
            case None =>
              println("Please provide valid email address " + to)
          }
        }

      }
      case None =>
        println("Enter a valid String for TO-email address list")

    }


    // Validating CC email string and all the email addresses passed
    validString(ccs) match {
      case Some(ccEmailString) => {
        if(ccEmailString!="NOT_SUPPLIED") {
          val ccList = ccEmailString.split(",", -1).toList
          for (cc <- ccList) {
             validateEmailAddress(cc) match {
              case Some(ccEmail) =>
                message.addRecipient(RecipientType.CC, new InternetAddress(ccEmail))
              case None =>
                println("Enter a valid String for CC-email address list")
            }

          }
        }
      }
      case None =>
        println("Please provide valid email address " + ccs)

    }
    message.setSubject(subject)
    message.setContent(emailBody, "text/html; charset=utf-8")
    try {
      Transport.send(message)
    }
    catch {
      case ex: Exception => {
        println("Exception occurred while sending the email" + ex)
      }
    }
  }


// Method to validate individual email addresses
  def validateEmailAddress(email: String) : Option[String] = {
    try {
      if ((email contains "@") &&  ( email contains ".")) {
        Some(email)
      }
      else {
        None
      }
    } catch {
      case ex: Exception => {
        println("Exception occured while validating the email address" + ex)
        None
      }
      }
  }

  // Method to validate the email list string passed.
  def validString (email: String) : Option[String] = {
    try {
      if (email.length() > 0)
        Some(email)
      else
        Some("NOT_SUPPLIED")
    }
    catch {
      case ex: Exception =>
      println("Exception in the email string supplied" + ex)
      None

    }
  }

/*// Testing the method.
  sendMail(fromEmailAddress,
    toEmailAddress,
    ccs = ccEmailAddress,
    emailSubject ,
    "Testing")*/
}
