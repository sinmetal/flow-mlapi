package org.sinmetal.mlapi;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2beta1.DlpServiceClient;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2beta1.*;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class DataLossPreventionFn extends DoFn<TableRow, TableRow> {

    static Logger logger = Logger.getLogger(DataLossPreventionFn.class.getSimpleName());

    @ProcessElement
    public void processElement(ProcessContext c) {
        Long id = parseLong(c.element().get("Id"));
        String message = c.element().get("Body").toString();

        DlpServiceClient dlp = null;
        try {
            dlp = DlpServiceClient.create();

            InspectConfig inspectConfig = InspectConfig.newBuilder()
                    .addAllInfoTypes(getInfoTypeList())
                    .build();
            String type = "text/plain";
            ContentItem itemsElement = ContentItem.newBuilder()
                    .setType(type)
                    .setValue(message)
                    .build();
            List<ContentItem> items = Arrays.asList(itemsElement);

            String inspectResponse = callInspectContent(dlp, inspectConfig, items);
            String maskValue = callDeidentify(dlp, inspectConfig, items);

            TableRow row = new TableRow();
            row.put("Id", id);
            row.put("Body", message);
            row.put("MaskBody", maskValue);

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
    }

    private List<InfoType> getInfoTypeList() {
        List<String> names = new ArrayList<>();
        names.add("EMAIL_ADDRESS");
        names.add("CREDIT_CARD_NUMBER");
        names.add("FIRST_NAME");
        names.add("LAST_NAME");
        names.add("IBAN_CODE");
        names.add("LOCATION");
        names.add("PERSON_NAME");
        names.add("PHONE_NUMBER");
        names.add("SWIFT_CODE");
        names.add("JAPAN_INDIVIDUAL_NUMBER");
        names.add("JAPAN_PASSPORT");

        List<InfoType> infoTypes = new ArrayList<>();
        for (String name : names) {
            InfoType infoTypesElement = InfoType.newBuilder()
                    .setName(name)
                    .build();
            infoTypes.add(infoTypesElement);
        }

        return infoTypes;
    }

    private String callInspectContent(DlpServiceClient dlp, InspectConfig inspectConfig, List<ContentItem> items) {
        InspectContentResponse response = dlp.inspectContent(inspectConfig, items);
        List<InspectResult> resultsList = response.getResultsList();
        List<Finding> findings = new ArrayList<>();
        for (InspectResult result : resultsList) {
            for (Finding finding : result.getFindingsList()) {
                findings.add(finding);
            }
        }

        Gson gson = new Gson();
        return gson.toJson(findings);
    }

    private String callDeidentify(DlpServiceClient dlp, InspectConfig inspectConfig, List<ContentItem> items) {
        CharacterMaskConfig maskConfig = CharacterMaskConfig.newBuilder().setMaskingCharacter("*").build();
        PrimitiveTransformation primitiveTransformation = PrimitiveTransformation.newBuilder().setCharacterMaskConfig(maskConfig).build();
        InfoTypeTransformations.InfoTypeTransformation infoTypeTransformation = InfoTypeTransformations.InfoTypeTransformation.newBuilder().setPrimitiveTransformation(primitiveTransformation).build();
        InfoTypeTransformations infoTypeTransformations = InfoTypeTransformations.newBuilder().addTransformations(infoTypeTransformation).build();
        DeidentifyConfig deidentifyConfig = DeidentifyConfig.newBuilder().setInfoTypeTransformations(infoTypeTransformations).build();

        DeidentifyContentResponse deidentifyContentResponse = dlp.deidentifyContent(deidentifyConfig, inspectConfig, items);
        List<ContentItem> itemsList = deidentifyContentResponse.getItemsList();
        if (itemsList.size() > 0) {
            return itemsList.get(0).getValue();
        }
        return "";
    }

    private Long parseLong(Object value) {
        if (value == null) {
            return null;
        }

        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            logger.info(String.format("failed ParseLong; value = %s", value));
            return null;
        }
    }
}