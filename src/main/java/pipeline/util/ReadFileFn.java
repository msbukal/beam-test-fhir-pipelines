package pipeline.util;

import static pipeline.util.ReportException.ERROR_TAG;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class ReadFileFn extends DoFn<ReadableFile, String> {
  public static final TupleTag<String> FILE_TAG = new TupleTag<>("file");

  private static final String JSON = "json";
  private static final String NDJSON = "ndjson";

  private void readWholeFile(ProcessContext ctx, ReadableFile file)
      throws IOException {
    String fileContents = file.readFullyAsUTF8String();
    ctx.output(fileContents);
  }

  private void readLines(ProcessContext ctx, ReadableFile file)
      throws IOException {
    try (BufferedReader r =
        new BufferedReader(new InputStreamReader(Channels.newInputStream(file.open())))) {
      String line;
      while ((line = r.readLine()) != null) {
        ctx.output(line);
      }
    }
  }

  @ProcessElement
  public void process(ProcessContext ctx) {
    ReadableFile file = ctx.element();
    String filename = file.getMetadata().resourceId().toString();

    int index = filename.lastIndexOf('.');
    if (index == -1) {
      return;
    }
    String extension = filename.substring(index + 1);

    try {
      if (extension.equals(JSON)) {
        readWholeFile(ctx, file);
      } else if (extension.equals(NDJSON)) {
        readLines(ctx, file);
      }
    } catch (IOException e) {
      ctx.output(ERROR_TAG, KV.of(filename, e));
    }
  }
}
