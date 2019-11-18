package com.cycloneboy.scala.spark.tempdata

import scalafx.application.JFXApp
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.FXRenderer

/**
 *
 * Create by  sl on 2019-11-18 20:03
 */
object PlotTest extends JFXApp {

  val x = Array(1 to 10)
  val y = Array(1 to 10)
  println(x)


  val plot = Plot.scatterPlots(Seq((1 to 10, 1 to 10, 0xffff0000, 5))
    , "Temp"
    , "Month"
    , "Temperature"
  )
  FXRenderer(plot, 800, 600)

}
