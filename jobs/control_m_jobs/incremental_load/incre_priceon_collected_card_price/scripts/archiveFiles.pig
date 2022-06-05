set output.compression.enabled true;
set output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

increFiles = LOAD '${rawFilePath}/ingestion_date=${processDate}/' USING PigStorage();
STORE increFiles INTO '${tableArchivePath}/' USING PigStorage();

rm ${rawFilePath}/ingestion_date=${processDate}