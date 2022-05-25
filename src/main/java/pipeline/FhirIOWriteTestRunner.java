package pipeline;

import static pipeline.util.ReadFileFn.FILE_TAG;
import static pipeline.util.ReportException.ERROR_TAG;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import pipeline.util.MatchFilesFn;
import pipeline.util.ReadFileFn;
import pipeline.util.ReportException;
import pipeline.util.ReportHealthcareIOError;

/**
 * FhirIOWriteTestRunner - testing modifications to FhirIO.Write.ExecuteBundles
 */
public class FhirIOWriteTestRunner {

  private static final Counter FILE_VALIDATION_ERRORS =
      Metrics.counter(FhirIOWriteTestRunner.class, "write_test/file_validation_error_count");

  private static final Counter FHIR_WRITE_ERRORS =
      Metrics.counter(FhirIOWriteTestRunner.class, "write_test/fhir_write_error_count");

  /**
   * Pipeline options
   */
  public interface FhirIOWriteTestRunnerOptions extends PipelineOptions {

    @Description(
        "The source directory for GCS files to read, including wild cards.")
    @Required
    ValueProvider<String> getSourceGcsLocation();

    void setSourceGcsLocation(ValueProvider<String> sourceGcsLocation);

    @Description(
        "The target FHIR Store to write data to, must be of the full format: "
            + "projects/project_id/locations/location/datasets/dataset_id/fhirStores/fhir_store_id")
    @Required
    ValueProvider<String> getFhirStore();

    void setFhirStore(ValueProvider<String> fhirStore);
  }

  public static Pipeline createPipeline(String[] args) {
    FhirIOWriteTestRunnerOptions opts =
        PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(FhirIOWriteTestRunnerOptions.class);

    Pipeline pipeline = Pipeline.create(opts);

    // Read files from GCS.
    PCollectionTuple fileContents =
        pipeline
            .apply(new MatchFilesFn(opts.getSourceGcsLocation().get()))
            .apply("ExtractFileContents",
                ParDo.of(new ReadFileFn()).withOutputTags(FILE_TAG, TupleTagList.of(ERROR_TAG)));

    fileContents
        .get(ERROR_TAG)
        .apply("ReportFileErrors", ParDo.of(new ReportException(FILE_VALIDATION_ERRORS)));

    // Execute bundles.
    FhirIO.Write.Result results =
        fileContents.get(FILE_TAG).apply(FhirIO.Write.executeBundles(opts.getFhirStore()));

    results.getFailedBodies()
        .apply("ReportWriteErrors", ParDo.of(new ReportHealthcareIOError(FHIR_WRITE_ERRORS)));

    return pipeline;
  }

  public static void main(String[] args) {
    createPipeline(args).run();
  }
}
