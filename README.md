# beam-test-pipelines

To test local modifications to FhirIO, I copy package `org.apache.beam.sdk.io.gcp.healthcare` into `src/main/java/`.

## FhirIOImportTestRunner

Batch pipeline, reads resources from GCS and then calls FhirIO.Import.

eg. command to run
```
SOURCE_GCS_LOCATION=gs://${BUCKET_NAME?}/{...}/**
DEAD_LETTER_LOCATION=gs://${BUCKET_NAME?}/{...}//tmp/ 
FHIR_STORE=projects/${PROJECT?}/locations/${LOCATION?}/datasets/${DATASET?}/fhirStores/${FHIR_STORE?}

./gradlew shadowJar -PmainClass=pipeline.FhirIOImportTestRunner
java -jar build/libs/pipe-1.0-SNAPSHOT-all.jar --sourceGcsLocation=${SOURCE_GCS_LOCATION?} --deadLetterGcsLocation=${DEAD_LETTER_LOCATION?} --fhirStore=${FHIR_STORE?} --project=${PROJECT?} --runner=DataflowRunner --region=${LOCATION?}
```

## FhirIOWriteTestRunner

Batch pipeline, reads resources from GCS and then calls FhirIO.ExecuteBundle.

eg. command to run
```
SOURCE_GCS_LOCATION=gs://${BUCKET_NAME?}/{...}/**
FHIR_STORE=projects/${PROJECT?}/locations/${LOCATION?}/datasets/${DATASET?}/fhirStores/${FHIR_STORE?}

./gradlew shadowJar -PmainClass=pipeline.FhirIOWriteTestRunner
java -jar build/libs/pipe-1.0-SNAPSHOT-all.jar --sourceGcsLocation=${SOURCE_GCS_LOCATION?} --fhirStore=${FHIR_STORE?} --project=${PROJECT?} --runner=DataflowRunner --region=${LOCATION?}
```

## FhirIOReadWriteTestRunner

Streaming pipeline, reads resources according to a PubSub source and then 
writes to another FHIR store using FhirIO.ExecuteBundles.

eg. command to run
```
PUBSUB_SUBSCRIPTION=projects/${PROJECT?}/subscriptions/${SUBSCRIPTION?}
FHIR_STORE=projects/${PROJECT?}/locations/${LOCATION?}/datasets/${DATASET?}/fhirStores/${FHIR_STORE?}

./gradlew shadowJar -PmainClass=pipeline.FhirIOReadWriteTestRunner
java -jar build/libs/pipe-1.0-SNAPSHOT-all.jar --pubSubSubscription=${PUBSUB_SUBSCRIPTION?} --fhirStore=${FHIR_STORE?} --project=${PROJECT?} --runner=DataflowRunner --region=${LOCATION?}
```