package com.aws.hli

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import org.apache.spark._
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * This class takes a S3 bucket, traverses all the objects in the bucket,
  * and extracts meta object info for each.
  */

object S3PreProcessedIndex {

  /**
    * list all objects recursively in a bucket,
    * Filter objects for prefix if any
    * @param bucket Bucket to list all the objects
    * @param prefix the prefix if any
    * @return list of s3 objects in a bucket
    */
  def s3List(bucket: String, prefix: String = "")
          : java.util.List[S3ObjectSummary] = {
    val s3Client = new AmazonS3Client(
                      new DefaultAWSCredentialsProviderChain)

    var current = s3Client.listObjects(bucket, prefix)
    val keyList = current.getObjectSummaries
    current = s3Client.listNextBatchOfObjects(current)
    while( current.isTruncated ) {
      keyList.addAll(current.getObjectSummaries)
      current = s3Client.listNextBatchOfObjects(current)
    }
    keyList.addAll(current.getObjectSummaries)
    println(keyList.size + " objects in " + bucket + "/" + prefix + " found.")
    keyList
  }


  /**
    * Exclude objects from list that startsWith the
    * exclude pattern
    * @param bucket bucket name
    * @param prefix a specific prefix
    * @param exclude string to match beginning of object name from the list
    * @return list of filtered s3 objects.
    */
  def excludeList( bucket: String,
                    prefix: String = "",
                    exclude: String = ""): java.util.List[S3ObjectSummary] = {
    s3List(bucket, prefix).filter { obj => ! obj.getKey.startsWith(exclude) }
  }

  
  /** Read contents of access log to extract last accessed time
    * and action performed on the object, e.g. PUT
    * @param sc sparkcontext
    * @param bucket bucket name
    * @param prefix filter by prefix "logs"
    * @return rdd of action, timestamp for each s3 object
    */
  def logRdd( sc: SparkContext,
              bucket: String,
              prefix: String = "") :
      org.apache.spark.rdd.RDD[(String, String)] = {
    sc.parallelize(s3List(bucket,prefix),1000).map{ key =>
      val s3Client = new AmazonS3Client(
                      new DefaultAWSCredentialsProviderChain)
      println("Reading key:"+bucket+"/"+key.getKey)
      val s3Object = s3Client.getObject(bucket, key.getKey)
      val contents: InputStream = s3Object.getObjectContent
      val lines = Source.fromInputStream(contents).getLines()

      val logout = lines.map( line => {
          println( "Processing log item: " + line ) 
          val l_rx = """([^\s"\[\]]+|\[[^\]\[]+\]|"[^"]+")""".r
          val tokens = l_rx.findAllIn(line).toArray
          if( tokens.length >= 16 ) {
            Array(  tokens(7), // object
                    tokens(2), // DateTimestamp
                    tokens(3), // remoteIp
                    tokens(4), // requester arn
                    tokens(5), // requesterId
                    tokens(6), //operation
                    tokens(17) //version
                  ).mkString(",")
          } else {
            println("ERRRRROOOORRRRRR => " + line)
            "ERRRRROOOORRRRRR => " + line
          }
        }
      )
      logout
    }.flatMap(l=>l).keyBy ( str => str.split(",")(0) )
  }

  /** Return list of s3 files from bucket 
    * @param sc sparkContext
    * @param bucket list obhects from this bucket.
    * @return list of s3 objects
    */
  def s3ObjectsRdd( sc: SparkContext,
                  bucket: String,
                  logPrefix: String) :
      org.apache.spark.rdd.RDD[(String, String)] = {

    sc.parallelize(excludeList(bucket,"",logPrefix)).map{ p =>
      val epoch = p.getLastModified.getTime
      val ext = p.getKey.split("/").last.split("\\.")
      val extension = if ( ext.length > 1 ) ext(ext.length - 1) else "unknown"
      Array(  p.getKey,
              extension,
              p.getKey.split("/").last,
              p.getKey.split("/").init.mkString("/"),
              bucket,
              p.getOwner.getDisplayName,
              epoch,
              p.getSize,
              p.getETag,
              p.getStorageClass)
      .mkString(",")
    }.keyBy ( str => str.split(",")(0) )

  }

  def prepedObjects( sc: SparkContext,
                  bucket: String,
                  partFile: String,
                  logPrefix: String) :
      org.apache.spark.rdd.RDD[(String, String)] = {

    println("Processing part file:"+partFile)
    sc.textFile(partFile).map{ line =>

      println("Processing line : " + line )
      val data = line.split(",")
      if( data.length > 3 && data.length <= 6 ) {
        val objectKey = data(0)
        val keyParts  = objectKey.split("/")
        val ext = keyParts.last.split("\\.")
        val extension = if ( ext.length > 1 ) ext(ext.length - 1) else "unknown"

        println("Processing objectKey:"+ objectKey + ":" + keyParts + ":" + extension)
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val date = sdf.parse( data(3).replace("T"," ").replace("Z","") )
        val epoch = date.getTime

        Array(  objectKey,
                extension,
                objectKey.split("/").last,
                objectKey.split("/").init.mkString("/"),
                bucket,
                data.last,
                epoch,
                data(2),
                data(1),
                data(4))
        .mkString(",")
      } else {
        line + " COLUMN MISMATCH"
      }
    }.keyBy ( str => str.split(",")(0) )

  }


  /**
    * Driver for generating Dynamodb output
    * @param sc SparkContext
    * @param inputBucket input bucket name
    * @param logBucket input bucket name
    * @param outputBucket input bucket name
    */
  def dynamoDBImporter( sc: SparkContext,
                        inputBucket: String,
                        logBucket: String,
                        logPrefix: String,
                        outputBucket: String,
                        prepedFile: String) : Unit = {
    println("Using preped data")
    val objectList = prepedObjects(sc, inputBucket, prepedFile ,logPrefix)
    println("Using access log data")
    val logList = logRdd(sc,logBucket,logPrefix)

    val datePath = new SimpleDateFormat("mmHHss_ddMMyyyy")
                  .format(Calendar.getInstance.getTime)

    val outPath = "s3n://" + outputBucket + "/" +
                  inputBucket + "/import-to-dynamodb/" + datePath

    println("Creating join")
    val flattened = objectList.leftOuterJoin(logList)
    

    println("Processing join")
    flattened.map { s =>
      println("JOIN row:" + s)
      val k = inputBucket + "/" + s._1
      val r1 = s._2._1
      val r2 = s._2._2

      val metaArray = r1.split(",")
      val accessArray = r2.getOrElse("").split(",")
      println("R2 from join  in :" + r2)
      println("R2 array :" + accessArray(0))
      var ( modifiedDt,remoteIp,requester,requestId,appName,op, version) =
                ("-","-","-","-","-","-","-")
      if( accessArray.length > 6 ) {
        modifiedDt = accessArray(1)
        remoteIp = accessArray(2)
        requester = accessArray(3)
        requestId = accessArray(4)
        appName = "-"
        op = accessArray(5)
        version = accessArray(6)
      }

      if( metaArray.length > 9 ) {
        "{ \"BucketKey\" : \""            + k + "\", " +
          "\"VersionId\" : \""            + version + "\", " +
          "\"FileType\" : \""             + metaArray(1) + "\", " +
          "\"EpochTimestamp\" : "         + metaArray(6) + ", " +
          "\"ObjectSize\" : "             + metaArray(7) + ", " +
          "\"Etags\" : \""                + metaArray(8) + "\", " +
          "\"StorageType\" : \""          + metaArray(9) + "\", " +
          "\"BucketOwner\" : \""          + metaArray(5) + "\", " +
          "\"DateTimestamp\" : \""        + modifiedDt + "\", " +
          "\"RemoteIp\" : \""             + remoteIp + "\", " +
          "\"Requester\" : \""            + requester + "\", " +
          "\"RequestId\" : \""            + requestId + "\", " +
          "\"ApplicationName\" : \""      + appName + "\", " +
          "\"ProcessingType\" : \""       + "EMR" + "\", " +
          "\"Operation\" : \""            + op  + "\"" +
        "}"
      }
    }.saveAsTextFile(outPath)
    //flattened.take(20).foreach(println) // for debug only

  }


  def main(args: Array[String]) : Unit = {
    if ( args.length != 2 ) {
      println("Usage: spark-submit --class com.aws.hli.S3PreProcessedIndex " + 
                      "--master yarn-cluster hli-indexer.jar " +
                      "<output-bucket> <s3-mapping-filepath>" )
    } else {

      val s3Reader = new AmazonS3Client(
                        new DefaultAWSCredentialsProviderChain())

      val mappingPath = args(1).split("/")
      val s3Object = s3Reader.getObject(mappingPath(0), mappingPath(1))

      val contents: InputStream = s3Object.getObjectContent
      val lines = Source.fromInputStream(contents).getLines()

      val sc = new SparkContext(new SparkConf().setAppName("S3IndexTest"))
      lines.foreach { line =>
        val mapping = new JSONObject(line)
        println("PROCESSING INPUT BUCKET: "+ mapping.getString("dataBucket"))
        if( mapping.has("logBucket")) {
          if( mapping.has("logPrefix") ) {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("logBucket"),
                            mapping.getString("logPrefix"),
                            args(0), mapping.getString("prepedFile"))
          } else {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("logBucket"),"logs", 
                            args(0), mapping.getString("prepedFile"))
          }
        } else {
          if( mapping.has("logPrefix") ) {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("dataBucket"),
                            mapping.getString("logPrefix"),
                            args(0), mapping.getString("prepedFile"))
          } else {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("dataBucket"),"logs", 
                            args(0), mapping.getString("prepedFile"))
          }
        }
      }
      sc.stop
    }


  }

}
