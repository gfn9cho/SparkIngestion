package edf.utilities

class Cipher(message:String) {
  val cHARMIN = 32 //Lower bound
  val cHARMAX = 126 //Upper bound

  //"Wraps" a character, so it doesn't go under cHARMIN, or over cHARMAX
  //Takes an Int so the Char can't underflow prior to being passed in
  private def wrap(n:Int):Char =
    if (n > cHARMAX) (n - cHARMAX + cHARMIN).toChar
    else if (n < cHARMIN) (n - cHARMIN + cHARMAX).toChar
    else n.toChar

  private def forString(str:String)(f:Char => Char):String = {
    var newStr = ""
    for(c <- str) { newStr += f(c) }
    newStr
  }

  //Creates a repeated key that's at least as long as the message to be encrypted
  private def getRepKey(key:String):String =
    if (key.length >= message.length) key
    else key * (message.length / key.length + 1)

  //Modifies a letter in the message, based on the respective letter in the key,
  //    and the given binary operator
  private def modifyWithStringWith(f:(Int,Int) => Int)(strKey:String):String = {
    var newString = ""
    (message zip getRepKey(strKey)).foreach { case (c1,c2) =>
      newString += wrap( f(c1,c2) ) }
    newString
  }

  //Offsets each character in a String by a given offset
  def simpleOffset(offset:Int):String =
    forString(message) { (c:Char) =>
      wrap((c + offset).toChar)
    }

  //Encrypts/Decrypts using another String as the key
  def encryptWith(key:String):String =
    modifyWithStringWith(_+_)(key)

  def decryptWith(key:String):String =
    modifyWithStringWith(_-_)(key)
}

object Cipher {
  def apply(msg:String):Cipher = new Cipher(msg)
}
