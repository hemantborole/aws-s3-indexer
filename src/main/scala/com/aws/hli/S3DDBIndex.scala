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

object S3DDBIndex {

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

    //val keyList = List[S3ObjectSummary]()
    //level1List(bucket,prefix).map( prefix => {
      var current = s3Client.listObjects(bucket, prefix)
      //keyList.addAll( current.getObjectSummaries )
      val keyList = current.getObjectSummaries
      current = s3Client.listNextBatchOfObjects(current)
      while( current.isTruncated ) {
        keyList.addAll(current.getObjectSummaries)
        current = s3Client.listNextBatchOfObjects(current)
      }
      keyList.addAll(current.getObjectSummaries)
      println(keyList.size + " objects in " + bucket + "/" + prefix + " found.")
    //})
    keyList
  }

  /**
    * list all versioned objects recursively in a bucket,
    * Filter objects for prefix if any
    * @param bucket Bucket to list all the objects
    * @param prefix the prefix if any
    * @return list of s3 versioned objects in a bucket
    */
  def s3VersionedList(bucket: String, prefix: String = "")
          : java.util.List[S3VersionSummary] = {
    val s3Client = new AmazonS3Client(
                      new DefaultAWSCredentialsProviderChain)

    val versionedListRequest = new ListVersionsRequest()
                                .withBucketName(bucket)
                                .withPrefix(prefix)

    var current = s3Client.listVersions(versionedListRequest)
    val keyList = current.getVersionSummaries
    while( current.isTruncated ) {
      versionedListRequest.setKeyMarker(current.getNextKeyMarker)
      versionedListRequest.setVersionIdMarker(current.getNextVersionIdMarker)
      current = s3Client.listVersions(versionedListRequest)
      keyList.addAll(current.getVersionSummaries)
    }
    keyList.addAll(current.getVersionSummaries)
    println(keyList.size + " objects in " + bucket + "/" + prefix + " found.")
    keyList
  }

  def level1List(bucket: String, prefix: String = "")
          : java.util.List[String] = {
    val s3Client = new AmazonS3Client(
                      new DefaultAWSCredentialsProviderChain)

    println("Listing bucket:" + bucket)
    val prefixes = s3Client.listObjects(
            new ListObjectsRequest().withBucketName(bucket).withDelimiter("/")
        ).getCommonPrefixes
    println("Common prefixes:" + prefixes)
    val filtered = prefixes.filter( obj => {
              println("filtering object: " + obj )
              (! obj.startsWith( "lpierce" )) && ( ! obj.startsWith("lpiece") )
            })
    filtered
  }


  /**
    * Exclude objects from list that startsWith the
    * exclude pattern
    * @param bucket bucket name
    * @param prefix a specific prefix
    * @param exclude string to match beginning of object name from the list
    * @return list of filtered s3 versioned objects.
    */
  def excludeList( bucket: String,
                    prefix: String = "",
                    exclude: String = ""): java.util.List[S3VersionSummary] = {
    s3VersionedList(bucket, prefix).filter { obj => ! obj.getKey.startsWith(exclude) }
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
      val s3Object = s3Client.getObject(bucket, key.getKey)
      val contents: InputStream = s3Object.getObjectContent
      val lines = Source.fromInputStream(contents).getLines()

      val logout = lines.map( line => {
        println("Processing log data: " + line)
        val l_rx = """([^\s"\[\]]+|\[[^\]\[]+\]|"[^"]+")""".r
        val tokens = l_rx.findAllIn(line).toArray
        var version = "-"
        if( tokens.length > 7 ) {
          if( tokens.length >= 16 ) {
            version = tokens(17)
          }
          Array(tokens(7), // object - 0
                  tokens(2), // DateTimestamp - 1
                  tokens(3), // remoteIp - 2
                  tokens(4), // requester arn - 3
                  tokens(5), // requesterId - 4
                  tokens(6), //operation - 5
                  version, //version - 6
                  tokens(9), //http status code - 7
                  tokens(10) //error code - 8
                ).mkString(",")
        } else {
          println("ERRRRROOOORRRRRR => " + line)
          "ERRRRROOOORRRRRR => " + line
        }
      })
      logout
    }.flatMap(l=>l).keyBy ( str => {
      val bKey = str.split(",")(0)
      val oVersion = str.split(",")(8)
      bKey + ":" + oVersion
    })
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
      val versionId = if ( p.isLatest ) "-" else p.getVersionId
      Array(  p.getKey,                               //0
              extension,                              //1
              p.getKey.split("/").last,               //2
              p.getKey.split("/").init.mkString("/"), //3
              bucket,                                 //4
              p.getOwner.getDisplayName,              //5
              epoch,                                  //6
              p.getSize,                              //7
              p.getETag,                              //8
              p.getStorageClass,                      //9
              versionId)                         //10
      .mkString(",")
    }.keyBy ( str => {
      val bKey = str.split(",")(0)
      val oVersion = str.split(",")(10)
      bKey + ":" + oVersion
    })

  }

  def prepedObjects( sc: SparkContext,
                  bucket: String,
                  partFile: String,
                  logPrefix: String) :
      org.apache.spark.rdd.RDD[(String, String)] = {

    sc.textFile(partFile).map{ line =>

      val data = line.split(",")
      val objectKey = data(0)
      val keyParts  = objectKey.split("/")
      val ext = keyParts.last.split("\\.")
      val extension = if ( ext.length > 1 ) ext(ext.length - 1) else "unknown"

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val date = sdf.parse( keyParts(3).replace("T","").replace("z","") )
      val epoch = date.getTime

      Array(  objectKey,                                //0
              extension,                                //1
              objectKey.split("/").last,                //2
              objectKey.split("/").init.mkString("/"),  //3
              bucket,                                   //4
              data.last,                                //5
              epoch,                                    //6
              data(2),                                  //7
              data(1),                                  //8
              data(4))                                  //9
      .mkString(",")
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
                        outputBucket: String) : Unit = {
    val objectList = s3ObjectsRdd(sc, inputBucket, logPrefix)
    val logList = logRdd(sc,logBucket,logPrefix)

    val datePath = new SimpleDateFormat("mmHHss_ddMMyyyy")
                  .format(Calendar.getInstance.getTime)

    val outPath = "s3a://" + outputBucket + "/" +
                  inputBucket + "/import-to-dynamodb/" + datePath

    val flattened = objectList.leftOuterJoin(logList)
    

    flattened.map { s =>
      println("JOIN row:" + s)
      val k = inputBucket + "/" + s._1
      val r1 = s._2._1
      val r2 = s._2._2

      val metaArray = r1.split(",")
      val accessArray = r2.getOrElse("").split(",")
      println("R2 from join  in :" + r2)
      println("R2 array :" + accessArray(0))
      var ( modifiedDt,remoteIp,requester,requestId,appName,op, requestedVersion,
              latestVersion, httpStatusCode, errorCode) =
                ("-","-","-","-","-","-","-","-","-","-")
      if( accessArray.length > 1 ) {
        modifiedDt = accessArray(1)
        remoteIp = accessArray(2)
        requester = accessArray(3)
        requestId = accessArray(4)
        appName = "-"
        op = accessArray(5)
        requestedVersion = accessArray(6)
        latestVersion = metaArray(10)
        httpStatusCode = accessArray(7)
        errorCode = accessArray(8)
      }

      "{ \"bucket\" : \""               + inputBucket + "\", " +
        "\"bucketkey\" : \""            + metaArray(0) + "\", " +
        "\"requestedversion\" : \""     + requestedVersion + "\", " +
        "\"latestversion\" : \""        + latestVersion + "\", " +
        "\"filetype\" : \""             + metaArray(1) + "\", " +
        "\"lastupdated\" : "            + metaArray(6) + ", " +
        "\"objectsize\" : "             + metaArray(7) + ", " +
        "\"etags\" : \""                + metaArray(8) + "\", " +
        "\"storagetype\" : \""          + metaArray(9) + "\", " +
        "\"bucketowner\" : \""          + metaArray(5) + "\", " +
        "\"requesttime\" : \""          + modifiedDt + "\", " +
        "\"remoteip\" : \""             + remoteIp + "\", " +
        "\"requester\" : \""            + requester + "\", " +
        "\"requestid\" : \""            + requestId + "\", " +
        "\"applicationname\" : \""      + appName + "\", " +
        "\"processingtype\" : \""       + "EMR" + "\", " +
        "\"operation\" : \""            + op  + "\"," +
        "\"httpstatus\" : \""           + httpStatusCode  + "\"," +
        "\"errorcode\" : \""            + errorCode  + "\"" +
      "}"
    }.saveAsTextFile(outPath)
    //flattened.take(20).foreach(println) // for debug only

  }


  def main(args: Array[String]) : Unit = {
    if ( args.length != 2 ) {
      println("Usage: spark-submit --class com.aws.hli.S3DDBIndex " + 
                      "--master yarn-cluster hli-indexer.jar " +
                      "<output-bucket> <s3-mapping-filepath>" )
    } else {

      val s3Reader = new AmazonS3Client(
                        new DefaultAWSCredentialsProviderChain())

      val mappingPath = args(1).split("/")
      val s3Object = s3Reader.getObject(mappingPath(0), mappingPath(1))

      val contents: InputStream = s3Object.getObjectContent
      val lines = Source.fromInputStream(contents).getLines()

      //val sc = new SparkContext(new SparkConf().setAppName("S3IndexTest"))
      val sc = new SparkContext(new SparkConf().setAppName(args(1)))
      lines.foreach { line =>
        val mapping = new JSONObject(line)
        println("PROCESSING INPUT BUCKET: "+ mapping.getString("dataBucket"))
        if( mapping.has("logBucket")) {
          if( mapping.has("logPrefix") ) {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("logBucket"),
                            mapping.getString("logPrefix"),
                            args(0))
          } else {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("logBucket"),"logs", args(0))
          }
        } else {
          if( mapping.has("logPrefix") ) {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("dataBucket"),
                            mapping.getString("logPrefix"),
                            args(0))
          } else {
            dynamoDBImporter( sc, mapping.getString("dataBucket"),
                            mapping.getString("dataBucket"),"logs", args(0))
          }
        }
      }
      sc.stop

      /*println("Executing level1List:")
      level1List("hli-bix-us-west-2").foreach(println)
      */
    }


  }

}
