#!/usr/bin/env bash
#Author : Venkat Bhimireddy
#FileName : run-imdb-topxranks.sh
#1. properties file for file paths ${basedir}/imdb-dataset.properties
#2. resourceManager -(NA - for local file system)  if the job is running on a cluster and data is loaded to a filesysten like HDFS , this needs to be passed to spark to read the data from filesystem.
#4. titleType - for example "movie" , there are different types of titles, like "short","tvSeries","tvMiniSeries","tvMovie","tvEpisode","tvSpecial", "video" .. etc
#5. BASEDIRFS - is dataset directory , forexample =>  ${RM}+${BASEDIRFS}/imdb-dataset/title.ratings.tsv,  ${RM}+ ${BASEDIRFS}/imdb-dataset/title.crew.tsv .. etc.
#6. topXRanks - If user wants to get the result for top 20 ranked titles , or top 50 ranked titles, this can be 20, 25, 50.. any number.
#7. outputPath - this is for the result of the ranked titles to save in a csv file.
#8. BASEDIR - is for the reading local filesytem - for jar location/ properties file location.
#9. SPARK-MODE - local[*] ,this value is currently running local - this can be changed to either "yarn-client"/ "yarn-cluster"

BASEDIR=/Users/${USER} \
BASEDIRFS=/Users/${USER} \
PROPERTIESPATH=${BASEDIR}/IdeaProjects/IMDBDataAnalysis/src/main/resources \
RM="NA" \
JARLOCPATH=${BASEDIR}/IdeaProjects/IMDBDataAnalysis/target \
TITLETYPE="movie" \
TOPXRANKS=20 \
OUTPUTPATH=${BASEDIR}/imdb-dataset/output \

./spark-submit --master local[*] \
--num-executors 4 \
--executor-memory 2G \
--driver-memory 2G \
--executor-cores 2 \
--driver-java-options "-Dlog4j.configuration=-XX:MaxPermSize=1G -XX:+UseConcMarkSweepGC  -XX:+CMSClassUnloadingEnabled" \
--conf 'spark.driver.extraJavaOptions -Dlog4j.configuration=-XX:MaxPermSize=1G -XX:+UseConcMarkSweepGC  -XX:+CMSClassUnloadingEnabled' \
--conf 'spark.executor.extraJavaOptions -Dlog4j.configuration=-XX:MaxPermSize=1G -XX:+UseConcMarkSweepGC  -XX:+CMSClassUnloadingEnabled' \
--conf spark.default.parallelism=50 \
--conf spark.shuffle.service.enabled=true \
--conf spark.sql.unsafe.enabled=true \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.driver.memoryOverhead=1024 \
--conf spark.sql.parquet.binaryAsString=true \
--conf spark.kryoserializer.buffer.max=1g \
--conf spark.storage.memoryFraction=0.7 \
--conf spark.rdd.compress=true \
--conf spark.broadcast.compress=true \
--conf spark.shuffle.spill=true \
--conf spark.network.timeout=900 \
--conf spark.rpc.askTimeout=900 \
--class org.code.vintrend.driver.IMDBAnalysisDriver \
${JARLOCPATH}/IMDBDataAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar \
${PROPERTIESPATH}/imdb-dataset.properties \
${RM} \
${BASEDIRFS} \
${TITLETYPE} \
${TOPXRANKS} \
${OUTPUTPATH}