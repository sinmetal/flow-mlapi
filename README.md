# flow-mlapi

Call DataLossPrevention API Sample.
BigQueryにあるTableのデータをDLP APIにかけて、結果をBigQueryのTableに保存する。

### Run

```
mvn compile exec:java -Dexec.mainClass=org.sinmetal.mlapi.MLApi -Dexec.args="--runner=DataflowRunner --project=sinmetal-samples --tempLocation=gs://temp-sinmetal-samples/tmp --gcpTempLocation=gs://temp-sinmetal-samples/tmp" -Pdataflow-runner
```
