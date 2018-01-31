package org.sinmetal.mlapi;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2beta1.DlpServiceClient;
import com.google.privacy.dlp.v2beta1.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class DataLossPreventionFn extends DoFn<TableRow, TableRow> {

    static Logger logger = Logger.getLogger(DataLossPreventionFn.class.getSimpleName());

    @ProcessElement
    public void processElement(ProcessContext c) {
        ObjectMapper m = new ObjectMapper();

        String message = c.element().get("Body").toString();
        logger.info("hoge-message=" + message);

        DlpServiceClient dlp = null;
        try {
            dlp = DlpServiceClient.create();
            String name = "EMAIL_ADDRESS";
            InfoType infoTypesElement = InfoType.newBuilder()
                    .setName(name)
                    .build();
            List<InfoType> infoTypes = Arrays.asList(infoTypesElement);
            InspectConfig inspectConfig = InspectConfig.newBuilder()
                    .addAllInfoTypes(infoTypes)
                    .build();
            String type = "text/plain";
            //String value = "My email is example @example.com.";
            ContentItem itemsElement = ContentItem.newBuilder()
                    .setType(type)
                    .setValue(message)
                    .build();
            List<ContentItem> items = Arrays.asList(itemsElement);
            InspectContentResponse response = dlp.inspectContent(inspectConfig, items);

            List<InspectResult> resultsList = response.getResultsList();

            String json = m.writeValueAsString(resultsList);
            TableRow row = new TableRow();
            row.put("DLPResponse", json);
            logger.info("dlp-response=" + json);
            c.output(row);
        }  catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            if (dlp != null) {
                try {
                    dlp.close();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }
//        try (DlpServiceClient dlp = DlpServiceClient.create()) {
//            String name = "EMAIL_ADDRESS";
//            InfoType infoTypesElement = InfoType.newBuilder()
//                    .setName(name)
//                    .build();
//            List<InfoType> infoTypes = Arrays.asList(infoTypesElement);
//            InspectConfig inspectConfig = InspectConfig.newBuilder()
//                    .addAllInfoTypes(infoTypes)
//                    .build();
//            String type = "text/plain";
//            //String value = "My email is example @example.com.";
//            ContentItem itemsElement = ContentItem.newBuilder()
//                    .setType(type)
//                    .setValue(message)
//                    .build();
//            List<ContentItem> items = Arrays.asList(itemsElement);
//            InspectContentResponse response = dlp.inspectContent(inspectConfig, items);
//
//            List<InspectResult> resultsList = response.getResultsList();
//
//            String json = m.writeValueAsString(resultsList);
//            TableRow row = new TableRow();
//            row.put("DLPResponse", json);
//            logger.info("dlp-response=" + json);
//            c.output(row);
//        } catch (Throwable e) {
//            throw new RuntimeException(e);
//        }
    }
}
