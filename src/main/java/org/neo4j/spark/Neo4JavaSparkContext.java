package org.neo4j.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mh
 * @since 01.03.16
 */
public class Neo4JavaSparkContext {

    private final SparkContext sc;
    private final SQLContext sqlContext;

    protected Neo4JavaSparkContext(SparkContext sc) {
        this.sc = sc;
        sqlContext = new SQLContext(sc);
    }

    public static Neo4JavaSparkContext neo4jContext(SparkContext sc) {
        return new Neo4JavaSparkContext(sc);
    }

    public static Neo4JavaSparkContext neo4jContext(JavaSparkContext sc) {
        return new Neo4JavaSparkContext(sc.sc());
    }

    public JavaRDD<Map<String,Object>> query(final String query, final Map<String,Object> parameters) {
        return Neo4jJavaIntegration.tupleRDD(sc, query, parameters);
    }

    public JavaRDD<Row> queryRow(final String query, final Map<String,Object> parameters) {
        return Neo4jJavaIntegration.rowRDD(sc, query, parameters);
    }

    public Dataset<Row> queryDF(final String query, final Map<String,Object> parameters, String...resultSchema) {
        if (resultSchema.length %2 != 0) throw new RuntimeException("Schema information has to be supplied as pairs of columnName,cypherTypeName (INTEGER,FLOAT,BOOLEAN,STRING,NULL)");
        int entries = resultSchema.length / 2;
        Map<String,String> schema = new LinkedHashMap<String,String>(entries);
        for (int i = 0; i < entries; i++) {
            schema.put(resultSchema[i * 2], resultSchema[i * 2 + 1].toUpperCase());
        }
        return Neo4jJavaIntegration.dataFrame(sqlContext, query,parameters, schema);
    }

    public Dataset<Row> queryDF(final String query, final Map<String,Object> parameters) {
        return Neo4jDataFrame.apply(sqlContext, query,parameters);
    }
}
