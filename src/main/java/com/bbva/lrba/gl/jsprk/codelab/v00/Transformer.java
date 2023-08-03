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

       // Dataset<Row> dataset1 = datasetsFromRead.get("sourceAlias1");
        //Dataset<Row> dataset2 = datasetsFromRead.get("sourceAlias2");
        //dataset1.show();
        //dataset2.show();

        //System.out.println("Dataset1 Counting: " + dataset1.count());
        //System.out.println("Dataset2 Counting: " + dataset2.count());

        //Dataset<Row> joinDNIDataset = dataset1.join(dataset2, "DNI");

        //Dataset<Row> exerciseDataset = joinDNIDataset.withColumn("FECHA", lit(current_date()))
                //.withColumn("CODITION", when(col("DNI").equalTo(1), 200).otherwise(40))
                //.withColumn("DNI", col("DNI").cast("Integer"));


        //Dataset<Row> selectDNIDataset = joinDNIDataset.select("DNI").where(joinDNIDataset.col("DNI").equalTo("000001"));
        ///datasetsToWrite.put("joinDNIDataset", exerciseDataset);
        //selectDNIDataset.show();


        //datasetsToWrite.get("joinDNIDataset").show();
        //exerciseDataset.groupBy("FECHA").sum("DNI").drop("FECHA").show();
        //exerciseDataset.groupBy("FECHA").avg("DNI").drop("FECHA").show();
        //exerciseDataset.groupBy("FECHA").max("DNI").drop("FECHA").show();

        //datasetsToWrite.put("targetAlias1", joinDNIDataset);

        //return datasetsToWrite;


            Dataset<Row> dataset1 = datasetsFromRead.get("sourceAlias1");

            Dataset<Row> datasetNEW = datasetsFromRead.get("sourceAlias3");

            Dataset<Row> dataSet3 = dataset1.join(datasetNEW, "DNI");
            dataSet3.show();
            dataSet3 = dataSet3.drop("DNI");

            datasetsToWrite.put("targetAlias1", dataSet3);

            return datasetsToWrite;
    }
}