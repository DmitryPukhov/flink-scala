package dmitry.examples.flink.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}

/**
  * Created by dima on 7/11/17.
  * Example of working with Flink streaming windows
  */
object WindowExample {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  /**
    * App entry point
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._

    // Create input stream
    val input = env.fromElements(
      ("home", "dog"),
      ("home", "cat"),
      ("wild", "wolf"),
      ("wild", "leopard"))

    // Each global window is animal category.
    // We accumulate animals there and calculate counts
    // Just for playing with windows, not optimal way of aggregation
    val ds: DataStream[(String, Int)] = input.keyBy(0)
      .window(GlobalWindows.create())
      .trigger(new OnNewElementTrigger[Any, GlobalWindow])
      .apply((key, window, input, out) => {

        out.collect((key.getField(0), input.toSeq.length))
    })
    ds.print()

    env.execute()

  }

  /**
    * Flink default trigger does nothing, so need to implement a custom one
    * to start window each new element
    */
  class OnNewElementTrigger[T, W <: Window] extends Trigger[T, W] {
    override def onElement(element: T, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = TriggerResult.FIRE

    override def clear(window: W, ctx: TriggerContext): Unit = {}

    override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = TriggerResult.CONTINUE
  }


}