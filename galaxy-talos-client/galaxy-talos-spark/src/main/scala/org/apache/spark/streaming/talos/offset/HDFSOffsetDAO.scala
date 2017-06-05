package org.apache.spark.streaming.talos.offset

import java.io._
import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.talos.TopicPartition
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkException}

private[talos] class HDFSOffsetDAO(
  offsetDir: String,
  hadoopConf: Configuration
) extends OffsetDAO with Logging {
  private val PREFIX = "offset-"
  private val REGEX = (PREFIX + """([\d]+)""").r
  private val MAX_ATTEMPTS = 3
  private lazy val executor = Executors.newFixedThreadPool(1)
  private var stopped = false
  private var fs_ : FileSystem = _

  private def fs = synchronized {
    if (fs_ == null) fs_ = new Path(offsetDir).getFileSystem(hadoopConf)
    fs_
  }

  private def reset() = synchronized {
    fs_ = null
  }

  def stop(): Unit = synchronized {
    if (stopped) return

    executor.shutdown()
    val terminated = executor.awaitTermination(10, TimeUnit.SECONDS)
    if (!terminated) {
      executor.shutdownNow()
    }
    logInfo(s"${this.getClass.getSimpleName} stopped.")
    stopped = true
  }

  private[talos] def doSave(time: Time, offsets: Map[TopicPartition, Long]): Unit = {
    var attempts = 0
    val tempFile = new Path(offsetDir, "temp")
    val offsetFile = new Path(offsetDir, PREFIX + time.milliseconds)

    while (attempts < MAX_ATTEMPTS && !stopped) {
      attempts += 1
      try {
        // Write to temp file
        if (fs.exists(tempFile)) {
          fs.delete(tempFile, true)
        }
        val fos = fs.create(tempFile)
        Utils.tryWithSafeFinally {
          val toSave = offsets.map { case (tp, offset) => (tp.asTuple, offset) }
          fos.write(HDFSOffsetDAO.serialize(toSave))
        } {
          fos.close()
        }

        // Rename to offset file
        if (fs.exists(offsetFile)) {
          fs.delete(offsetFile, true)
        }
        if (!fs.rename(tempFile, offsetFile)) {
          logWarning(s"Could not rename ${tempFile} to ${offsetFile}")
        }

        // Delete old offset files
        val allOffsetFiles = getOffsetFiles()
        if (allOffsetFiles.size > 10) {
          allOffsetFiles.take(allOffsetFiles.size - 10).foreach { file =>
            logInfo("Deleting " + file)
            fs.delete(file, true)
          }
        }

        // All done, print success
        logInfo(s"Saved offsets for time ${time.milliseconds} to file ${offsetFile}:\n" +
          s"${offsets.toSeq.sortBy(_._1.toString).mkString(",")}")
        return
      } catch {
        case t: Throwable =>
          logWarning(s"Error in attempt ${attempts} of saving offsets to ${offsetFile}.", t)
          reset()
      }
      logWarning(s"Could not write checkpoint for time $time to file $offsetFile")
    }
  }

  override def save(time: Time, offsets: Map[TopicPartition, Long]): Unit = {
    val writeOffsetHandler = new Runnable {
      override def run(): Unit = {
        doSave(time, offsets)
      }
    }

    try {
      executor.submit(writeOffsetHandler)
      logInfo(s"Submitted saving offsets for time ${time.milliseconds}.")
    } catch {
      case rej: RejectedExecutionException =>
        logError("Could not submit saving offsets task to the thread pool executor.", rej)
    }
  }

  override def restore(): Option[Map[TopicPartition, Long]] = {

    val offsetFiles = getOffsetFiles().reverse
    if (offsetFiles.isEmpty) {
      return None
    }

    logInfo("Offsets files found: " + offsetFiles.mkString(","))
    var readError: Exception = null
    offsetFiles.foreach { file =>
      logInfo("Attempting to load offsets from file " + file)
      try {
        val fis = fs.open(file)
        val bytes = IOUtils.toByteArray(fis)
        val offsetMap = HDFSOffsetDAO.deserialize[Map[(String, Int), Long]](bytes)
        logInfo(s"Restored offsets successfully from file ${file}:\n" +
          s"${offsetMap.toSeq.sortBy(_._1.toString).mkString(",")}")
        val result = offsetMap.map { case ((topic, partition), offset) =>
          (TopicPartition(topic, partition), offset)
        }
        return Some(result)
      } catch {
        case e: Exception =>
          readError = e
          logWarning("Error restoring offsets from file " + file, e)
      }
    }
    throw new SparkException(
      s"Failed to restore offsets from directory $offsetDir.", readError)
  }

  /** Get checkpoint files present in the given directory, ordered by oldest-first */
  private[streaming] def getOffsetFiles(): Seq[Path] = {
    def sortFunc(path1: Path, path2: Path): Boolean = {
      val time1 = path1.getName match {
        case REGEX(x) => x.toLong
      }
      val time2 = path2.getName match {
        case REGEX(x) => x.toLong
      }
      time1 < time2
    }

    val path = new Path(offsetDir)
    if (fs.exists(path)) {
      val statuses = fs.listStatus(path)
      if (statuses != null) {
        val paths = statuses.map(_.getPath)
        val filtered = paths.filter(p => REGEX.findFirstIn(p.toString).nonEmpty)
        filtered.sortWith(sortFunc).toSeq
      } else {
        logWarning("Listing " + path + " returned null.")
        Seq.empty
      }
    } else {
      logInfo("Offset directory " + path + " does not exist.")
      Seq.empty
    }
  }
}

object HDFSOffsetDAO {
  def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    Utils.tryWithSafeFinally {
      oos.writeObject(t)
    } {
      oos.close()
    }
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
        // scalastyle:on classforname
      }
    }
    Utils.tryWithSafeFinally {
      ois.readObject.asInstanceOf[T]
    } {
      ois.close()
    }
  }
}
