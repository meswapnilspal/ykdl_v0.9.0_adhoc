set output.compression.enabled true;
set output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

increFiles = LOAD '${processedFilePath}/ingestion_date=${yesterday}/' USING PigStorage();
STORE increFiles INTO '${tableArchivePath}/' USING PigStorage();
