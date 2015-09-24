package edu.umkc.sce.RTA4

/**
 * Created by mali on 9/23/2015.
 */

import java.io.{PrintStream, IOException}
import java.net.{Socket, InetAddress}

object Dispatcher {

  def findIpAdd():String =
  {
    val localhost = InetAddress.getLocalHost
    val localIpAddress = localhost.getHostAddress

    return localIpAddress
  }
  def dispatchCommand(string: String)
  {
    // Simple server

    try {


      lazy val address: Array[Byte] = Array(10.toByte, 205.toByte, 2.toByte, 163.toByte)
      val ia = InetAddress.getByAddress(address)
      val socket = new Socket(ia, 1234)
      val out = new PrintStream(socket.getOutputStream)
      //val in = new DataInputStream(socket.getInputStream())

      out.print(string)
      out.flush()

      out.close()
      //in.close()
      socket.close()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }


}
