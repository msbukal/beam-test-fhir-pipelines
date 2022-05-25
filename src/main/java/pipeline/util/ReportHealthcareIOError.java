package pipeline.util;

import org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportHealthcareIOError extends DoFn<HealthcareIOError<String>, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      ReportHealthcareIOError.class);

  private final Counter errorCounter;

  public ReportHealthcareIOError(Counter errorCounter) {
    this.errorCounter = errorCounter;
  }

  @ProcessElement
  public void process(ProcessContext ctx) {
    HealthcareIOError<String> error = ctx.element();
    LOGGER.error(errorToString(error));
    errorCounter.inc();
  }

  private String errorToString(HealthcareIOError<String> e) {
    String output = e.getErrorMessage();
    if (!e.getDataResource().isEmpty()) {
      output += String.format("Data Resource: %s\n", e.getDataResource());
    }
    if (!e.getStackTrace().isEmpty()) {
      output += String.format("StackTrace: %s\n", e.getStackTrace());
    }
    return output;
  }
}