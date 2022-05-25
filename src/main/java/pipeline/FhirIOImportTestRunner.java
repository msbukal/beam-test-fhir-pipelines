package pipeline;

import static pipeline.util.ReadFileFn.FILE_TAG;
import static pipeline.util.ReportException.ERROR_TAG;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import pipeline.util.MatchFilesFn;
import pipeline.util.ReadFileFn;
import pipeline.util.ReportException;
import pipeline.util.ReportHealthcareIOError;

/**
 * FhirIOImportTestRunner - test import changes from a FixIt
 */
public class FhirIOImportTestRunner {

  private static final Counter FILE_VALIDATION_ERRORS =
      Metrics.counter(FhirIOImportTestRunner.class, "import_test/file_validation_error_count");

  private static final Counter FHIR_IMPORT_ERRORS =
      Metrics.counter(FhirIOImportTestRunner.class, "import_test/fhir_import_error_count");

  /**
   * Pipeline options
   */
  public interface FixitBatchRunnerOptions extends PipelineOptions {

    @Description("The source directory for GCS files to read, including wildcards.")
    @Required
    ValueProvider<String> getSourceGcsLocation();

    void setSourceGcsLocation(ValueProvider<String> sourceGcsLocation);

    @Required
    ValueProvider<String> getDeadLetterGcsLocation();

    void setDeadLetterGcsLocation(ValueProvider<String> deadLetterGcsLocation);

    @Description(
        "The target FHIR Store to write data to, must be of the full format: "
            + "projects/project_id/locations/location/datasets/dataset_id/fhirStores/fhir_store_id")
    @Required
    ValueProvider<String> getFhirStore();

    void setFhirStore(ValueProvider<String> fhirStore);
  }

  public static Pipeline createPipeline(String[] args) {
    FixitBatchRunnerOptions opts =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FixitBatchRunnerOptions.class);

    Pipeline pipeline = Pipeline.create(opts);

    // Load GCS files into a PCollection
    PCollectionTuple fileContents =
        pipeline
            .apply(new MatchFilesFn(opts.getSourceGcsLocation().get()))
            .apply("ExtractFileContents",
                ParDo.of(new ReadFileFn()).withOutputTags(FILE_TAG, TupleTagList.of(ERROR_TAG)));

    fileContents
        .get(ERROR_TAG)
        .apply(
            "ReportFileErrors", ParDo.of(new ReportException(FILE_VALIDATION_ERRORS)));

    // Import that PCollection
    FhirIO.Write.Result results =
        fileContents.get(FILE_TAG)
            .apply(FhirIO.importResources(
                opts.getFhirStore(),
                StaticValueProvider.of(opts.getTempLocation()),
                opts.getDeadLetterGcsLocation(),
                ContentStructure.RESOURCE));

    results.getFailedBodies()
        .apply("ReportImportErrors", ParDo.of(new ReportHealthcareIOError(FHIR_IMPORT_ERRORS)));

    return pipeline;
  }

  public static void main(String[] args) {
    createPipeline(args).run();
  }
}