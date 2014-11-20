package util

import java.util.Date

trait Report {

  def graphLoadingStart: Date
  def graphLoadingEnd: Date
  def graphLoadingTime: Long

  def algorithExecutionStart: Date
  def algorithExecutionEnd: Date
  def algorithExecutionTime: Long

  def resultSavingStart: Date
  def resultSavingEnd: Date
  def resultSavingTime: Long

  def master: String

  def wholeTime : Long
  def name : String
}