package edf.utilities
import org.apache.log4j.Logger
object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
