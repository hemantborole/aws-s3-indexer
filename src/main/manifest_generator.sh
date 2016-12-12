#!/bin/bash 

root=s3://import-to-redshift
aws s3 ls ${root}/ |awk '{print $NF}' | while read prefix;do
  outname=`basename $prefix`
  echo '{ "entries": [' > ${outname}_manifest.json
  aws s3 ls --recursive $root/$prefix  | awk -v root=$root '{if($3 != 0 ) {printf("{\"url\":\"%s/%s\",\"mandatory\":true},\n",root, $NF)}}' |grep -v '_SUCCESS' >> ${outname}_manifest.json
  echo '  ] } ' >> ${outname}_manifest.json
done
