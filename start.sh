#!/bin/bash
cd /home/zinnion/
mc config host add minio https://storage.zinnion.com $MINIO_ACCESS_KEY $MINIO_SECRET_KEY
mc cp minio/app/$PKG.tar.gz .
tar -zxvf $PKG.tar.gz
npm install 
node main.js $DEX >> /dev/stdout 2>&1
