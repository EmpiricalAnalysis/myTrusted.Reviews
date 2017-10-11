#!/bin/bash

while read line; do
  if [ "${line:64:4}" = "revi" ]; then
     wget "${line}"
     filename="${line:64:30}"
     aws s3 cp $(echo $filename) s3://insightdenyc/reviews/
     rm $(echo $filename)
  fi
  if [ "${line:64:4}" = "meta" ]; then
     wget "${line}"
     filename="${line:64:30}"
     aws s3 cp $(echo $filename) s3://insightdenyc/meta/
     rm $(echo $filename)
  fi
  if [ "${line:64:4}" = "rati" ]; then
     wget "${line}"
     filename="${line:64:30}"
     aws s3 cp $(echo $filename) s3://insightdenyc/ratings/
     rm $(echo $filename)
  fi
done < "data_urls.txt"
