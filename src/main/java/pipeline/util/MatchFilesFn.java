package pipeline.util;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class MatchFilesFn extends PTransform<PBegin, PCollection<ReadableFile>> {

  private final String source;

  public MatchFilesFn(String source) {
    this.source = source;
  }

  @Override
  public PCollection<ReadableFile> expand(PBegin input) {
    return input
        .apply(
            "ReadGcsFiles",
            FileIO.match().filepattern(source)
                .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .apply(FileIO.readMatches());

  }
}