package com.bbva.lrba.gl.jsprk.codelab.v00;

import com.bbva.lrba.spark.transformers.Transform;
import com.bbva.lrba.gl.jsprk.codelab.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset1 = datasetsFromRead.get("sourceAlias1");

        Dataset<Row> datasetNEW = datasetsFromRead.get("sourceAlias3");

        Dataset<Row> dataSet3 = dataset1.join(datasetNEW, "DNI");
        dataSet3.show();
        dataSet3 = dataSet3.drop("DNI");

        datasetsToWrite.put("targetAlias1", dataSet3);

        return datasetsToWrite;
    }

}