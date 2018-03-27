package org.sinmetal.mlapi;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

public class MLApi {

    public interface CleansingOptions extends GcpOptions {
    }

    public static class DataLossPrevention extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> lines) {
            return lines.apply(ParDo.of(new DataLossPreventionFn()));
        }
    }

    public static void main(String[] args) {
        CleansingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CleansingOptions.class);
        Pipeline p = Pipeline.create(options);

        TableReference table = new TableReference();
        table.setProjectId("sinmetal-samples");
        table.setDatasetId("dlp");
        table.setTableId("Message");

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("Id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("Body").setType("STRING"));
        fields.add(new TableFieldSchema().setName("MaskBody").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        TableReference tableRef = new TableReference();
        tableRef.setProjectId("sinmetal-samples");
        tableRef.setDatasetId("dlp");
        tableRef.setTableId("DLPMessage");

        p.apply(BigQueryIO.readTableRows().from(table))
                .apply("DLP API", new DataLossPrevention())
                .apply(
                        BigQueryIO.writeTableRows()
                                .withSchema(schema)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .to(tableRef));
        p.run();
    }
}