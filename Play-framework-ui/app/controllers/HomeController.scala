package controllers

import javax.inject._

import play.api._
import play.api.data.Forms._
import play.api.mvc._
import play.api.data.{Form, _}
import model.SearchForm
import javax.inject.Inject
import play.api.i18n.I18nSupport
import play.api.i18n.MessagesApi
import views._



import scala.io.StdIn._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(val messagesApi: MessagesApi) extends Controller with I18nSupport  {

  val jobTitleForm = Form(
    mapping(
      "jobTitle" -> nonEmptyText,
      "year" -> number
    )(SearchForm.apply)(SearchForm.unapply)
  )

  def index = Action { implicit request =>
    Ok(html.index(jobTitleForm))
  }

  def createSearch = Action { implicit request =>

   jobTitleForm.bindFromRequest.fold(
      formWithErrors => {
        println("in error")
        BadRequest(html.index(formWithErrors))
      },
      form => {
        val buf = scala.collection.mutable.ArrayBuffer.empty[Array[String]]
        val lines = scala.io.Source.fromFile("/Users/MengHaoHsu/Salary-analysis-using-scala/Salary-analysis/out.txt").mkString.split("\n")
        for(i <-0 until lines.length){
          if(form.year!=0) {
            println("year")
            if (lines(i).toLowerCase().contains(form.jobTitle.toLowerCase()) && lines(i).toLowerCase.contains(form.year.toString())) {
              val str = scala.collection.mutable.ArrayBuffer.empty[String]
              val tem = lines(i).split(" ");
              for (j <- 0 until tem.length) {
                val tem2 = tem(j).split(":")
                str += tem2(1)
              }
              buf += str.toArray
            }
          }else{
            println("no year")
            if (lines(i).toLowerCase().contains(form.jobTitle.toLowerCase())) {
              val str = scala.collection.mutable.ArrayBuffer.empty[String]
              val tem = lines(i).split(" ");
              for (j <- 0 until tem.length) {
                val tem2 = tem(j).split(":")
                str += tem2(1)
              }
              buf += str.toArray
            }
          }
        }

        Ok(html.result(buf.toArray, form.jobTitle, form.year))
      })
  }



}
