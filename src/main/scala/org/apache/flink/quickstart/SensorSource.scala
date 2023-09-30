package org.apache.flink.quickstart

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {

  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    val curTime = Calendar.getInstance.getTimeInMillis

    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    while (running) {
      curFTemp.foreach( t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}
