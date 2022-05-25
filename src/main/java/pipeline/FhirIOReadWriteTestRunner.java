package pipeline;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import pipeline.util.ReportHealthcareIOError;

/**
 * FhirIOReadWriteTestRunner - reads from pub sub, writes to target store This is a streaming
 * pipeline
 */
public class FhirIOReadWriteTestRunner {

  private static final Counter READ_RESOURCE_ERRORS =
      Metrics.counter(FhirIOWriteTestRunner.class, "read_write_test/read_resource_error_count");

  private static final Counter FHIR_WRITE_ERRORS =
      Metrics.counter(FhirIOWriteTestRunner.class, "read_write_test/fhir_write_error_count");

  /**
   * Pipeline options
   */
  public interface PubSubReadRunnerOptions extends PipelineOptions {

    @Description(
        "The PubSub subscription to listen to, must be of the full format: "
            + "projects/project_id/subscriptions/subscription_id.")
    @Required
    String getPubSubSubscription();

    void setPubSubSubscription(String pubSubSubscription);

    @Description(
        "The target FHIR Store to write data to, must be of the full format: "
            + "projects/project_id/locations/location/datasets/dataset_id/fhirStores/fhir_store_id")
    @Required
    ValueProvider<String> getFhirStore();

    void setFhirStore(ValueProvider<String> fhirStore);
  }

  public static Pipeline createPipeline(String[] args) {
    PubSubReadRunnerOptions opts =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubReadRunnerOptions.class);

    Pipeline pipeline = Pipeline.create(opts);

    // Read the FHIR resource from the input pubsub message.
    FhirIO.Read.Result readResult =
        pipeline
            .apply(
                "ReadFhirResources",
                PubsubIO.readStrings().fromSubscription(opts.getPubSubSubscription()))
            .apply(FhirIO.readResources());

    readResult
        .getFailedReads()
        .apply("ReportFhirReadErrors",
            ParDo.of(new ReportHealthcareIOError(READ_RESOURCE_ERRORS)));

    // Turn resources into POST bundles.
    PCollection<String> bundles = readResult.getResources()
        .apply("ConvertToBundle", ParDo.of(new ConvertToBundle()));

    // Execute bundles.
    FhirIO.Write.Result results = bundles.apply(FhirIO.Write.executeBundles(opts.getFhirStore()));

    results.getFailedBodies()
        .apply("ReportWriteErrors", ParDo.of(new ReportHealthcareIOError(FHIR_WRITE_ERRORS)));

    return pipeline;
  }

  public static void main(String[] args) {
    createPipeline(args).run();
  }

  private static class ConvertToBundle extends DoFn<String, String> {

    private static final String TEMPLATE = "{"
        + " \"resourceType\": \"Bundle\","
        + " \"type\": \"batch\","
        + " \"entry\": ["
        + "   {"
        + "     \"request\": {"
        + "       \"method\": \"POST\","
        + "       \"url\": \"%s\""
        + "     },"
        + "     \"resource\": {%s}"
        + "   }"
        + " ]"
        + "}";

    @ProcessElement
    public String process(String input) {
      JsonObject inJson = JsonParser.parseString(input).getAsJsonObject();
      String resourceType = inJson.getAsJsonPrimitive("resourceType").getAsString();
      return String.format(TEMPLATE, resourceType, input);
    }
  }
}
