CREATE TABLE hli (
  bucket varchar(256) ENCODE LZO,
  bucketkey varchar(256) ENCODE LZO,
  versionid varchar(32) ENCODE LZO,
  filetype varchar(64) ENCODE LZO,
  lastupdated VARCHAR(32) ENCODE LZO,
  objectsize varchar(16) ENCODE LZO,
  etags varchar(39) ENCODE runlength,
  storagetype varchar(18) ENCODE BYTEDICT,
  bucketowner varchar(64) ENCODE LZO,
  requesttime varchar(64) ENCODE LZO,
  remoteip varchar(32) ENCODE LZO,
  requester varchar(128) ENCODE BYTEDICT,
  requestid varchar(64) ENCODE LZO,
  operation varchar(24) ENCODE BYTEDICT,
  applicationame varchar(32) ENCODE BYTEDICT,
  processingtype varchar(16) ENCODE BYTEDICT,
  httpstatus varchar(8) encode bytedict,
  errorcode varchar(8) encode bytedict,
  primary key ( BucketKey)
)
interleaved sortkey(requesttime, lastupdated);
