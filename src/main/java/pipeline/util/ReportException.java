package pipeline.util;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportException extends DoFn<KV<String, Exception>, Void> {
  public static final TupleTag<KV<String, Exception>> ERROR_TAG = new TupleTag<>("errors") {};
  private static final Logger LOGGER = LoggerFactory.getLogger(ReportException.class);

  private final Counter errorCounter;

  public ReportException(Counter errorCounter) {
    this.errorCounter = errorCounter;
  }

  @ProcessElement
  public void process(ProcessContext ctx) {
    Exception exception = ctx.element().getValue();
    String key = ctx.element().getKey();

    LOGGER.error(String.format("Error Message: %s\nSource: %s\nStacktrace: %s", exception.getMessage(), key, exception.getStackTrace().toString()));
    errorCounter.inc();
  }
}