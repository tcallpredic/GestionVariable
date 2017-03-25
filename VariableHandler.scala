package projectClass

import java.io.File

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.mutable.Map

/**
  * Created by sysop on 22/03/17.
  */
class VariableHandler(path:String,spark: SparkSession) {
  var description:String = ""
  var ListVariable:Map[String,String] = Map()
  var fileSaved:Boolean = false

  // Saisie de la Description
  def SetDescription():Boolean={
    description = ""
    var req =""
    println("Saisir le nom de la description:")
    print("> ")
    description = scala.io.StdIn.readLine()
    description match {
      case "" =>
        println("Le nom de la description ne peut être vide !")
        println("Voulez-vous ressaisir le nom de la description à nouveau (O/N) ?")
        print("> ")
        req = scala.io.StdIn.readLine()
        if (req == "O"){
          SetDescription()} else {
            false }
      case _ =>
        true
    }
  }

  // Chargement du Parquet existant
  def LoadDescription(): Boolean ={
    if(SetDescription()== true) {
      if (!CheckDescription()) {
        println("Cette description n'existe pas !")
        false
      } else {
        LoadParquetFile()
        true
      }
    } else {false}
  }

  // Lecture du Parquet
  def CheckDescription(): Boolean ={
    val parquetVarFile = new File(path + description + ".parquet")
    if(parquetVarFile.exists()){
      true
    }else{
      false
    }
  }

  // Création d'une nouvelle description
  def CreateDescription(): Boolean ={
    SetDescription()
    if(CheckDescription()== true){
      println("Cette description existe déjà !")
      false
    } else {
      ListVariable.clear()
      fileSaved = false
      true
    }
  }

  // Ajout d'un bloc
  def AddBloc(): String = {
    var nameB = ""
    var req = ""
    println("Saisir le nom du bloc :")
    print("> ")
    nameB = scala.io.StdIn.readLine()
    if(nameB == "") {
      println("Le nom de bloc ne peut pas être vide ! Saisir à nouveau (O/N)?")
      print("> ")
      req = scala.io.StdIn.readLine()
      if (req == "O") {
        AddBloc()
      } else { "" }
    } else {nameB}
  }

  // Ajout d'une rubrique
  def AddRubrique(): String  = {
    var nameR = ""
    var req = ""
    println("Saisir le nom de la rubrique :")
    print("> ")
    nameR = scala.io.StdIn.readLine()
    if (nameR == "") {
      println("Le nom de la rubrique ne peut pas être vide ! Saisir à nouveau (O/N)?")
      print("> ")
      req = scala.io.StdIn.readLine()
      if (req == "O") {
        AddRubrique()
      } else { "" }
    } else { nameR }
  }

  // Ajout d'une Variable
  def AddVariable(): Unit = {
    var bloc = ""
    var rub =""
    var nameV=""
    var req =""
    bloc = AddBloc()
    if (bloc !="")
      {
        rub = AddRubrique()
        if (rub !=""){
          println("Saisir le nom de la variable")
          print("> ")
          nameV = scala.io.StdIn.readLine()
          if(nameV==""){
            println("Le nom de la variable ne peut pas être vide ! Saisir à nouveau (O/N)?")
            print("> ")
            req = scala.io.StdIn.readLine()
            if (req == "O") {
              AddVariable()
              }
          } else {
            var variable = bloc + "." + rub + "." + nameV
            if (ListVariable.contains(variable)) {
              println("La variable existe deja ! Saisissez un autre nom (O/N) ?")
              print("> ")
              req = scala.io.StdIn.readLine()
              if (req =="O"){
                AddVariable()
              } else {
                println("Ajout de Rubrique annulée !")
                false
              }
            }
            ListVariable += (variable -> "")
            fileSaved = false
          }
        }
      }
  }

  // Suppression d'une Variable
  def RemoveVariable(): Unit ={
    var nameV=""
    var req =""
    println("Saisir le nom de la variable :")
    print("> ")
    nameV = scala.io.StdIn.readLine()
    if(!ListVariable.contains(nameV)){
      println("Nom de la variable non existant, saisissez à nouveau (O/N)?")
      print("> ")
      req = scala.io.StdIn.readLine()
      if(req == "O"){
        RemoveVariable()
      } else {
        println("Suppression de Variable annulée")
        false
      }
    }
    ListVariable -= nameV
    println("Variable supprimée !")
    fileSaved = false

  }

  // Assignation des Variables
  def SetVariableValue(): Unit ={
    var variable=""
    while(!ListVariable.contains(variable) || variable==""){
      if(!ListVariable.contains(variable) && variable!="")println("Cette variable n'existe pas!")
      println("Saisissez le nom de la variable:")
      print("> ")
      variable = scala.io.StdIn.readLine()
    }
    var valueV = ""
    while(valueV ==  ""){
      println("Saisir la valeur de la variable "+variable)
      print("> ")
      valueV = scala.io.StdIn.readLine()
    }
    ListVariable(variable) = valueV
    fileSaved = false

  }

  // Sauvegarder au format Parquet
  def SaveParquetFile(): Unit ={
    import spark.implicits._
    val variableDf = ListVariable.toList.toDF(Seq("variable","value"):_*)
    variableDf.write.mode(SaveMode.Overwrite).parquet( path + description + ".parquet")
    fileSaved = true
  }


  // Charger le fichier Parquet
  def LoadParquetFile(): Unit ={
    val variableDf = spark.read.parquet( path+description+".parquet")
    MapLoader(variableDf)
    println("----------------------------------------")
    println("Description chargée !")
    println("----------------------------------------")
  }

  // Chargement des Colonnes du DF
  def MapLoader(df:DataFrame): Unit ={
    //import df.sqlContext.implicits._
    ListVariable = df.rdd.map(row => Map(row.getAs[String]("variable")->row.getAs[String]("value"))).reduce(_++_)
  }

  // Check de l'état de sauvegarde
  def GetSavedStatus():Boolean={
    fileSaved
  }

  // Affichage de la liste des Variables
  def DisplayListVariable(): Unit ={
    if(!ListVariable.isEmpty){
      println("----------------------------------------")
      println("Liste des variables de la description : "+description)
      ListVariable.foreach((r:(String,String)) => println(r))
      println("----------------------------------------")
    }else{
      println("----------------------------------------")
      println("Liste des variables de la description : "+description +" est vide !")
      println("----------------------------------------")
    }
  }
}
