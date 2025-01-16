package fr.dawan.paris.big.data.cc;

import fr.dawan.paris.BigDataCC;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnedisDataSet {
    private static final Logger logger = LoggerFactory.getLogger(EnedisDataSet.class);

    private final SparkSession sparkSession;
    private final Dataset<Row> dataFrame;

    /**
     * Charge un fichier dans un Spark DataSet
     * @param sparkSession
     * @param url
     * @param tableName
     */
    public EnedisDataSet(SparkSession sparkSession, String url, String tableName) {
        this.sparkSession = sparkSession;

        this.dataFrame = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(url);

        this.dataFrame.createOrReplaceTempView(tableName);
        logger.info("Table '{}' créée dans la session Spark.", tableName);
    }

    public void afficherSchema() {
        StructType schema = dataFrame.schema();
        logger.info("Schéma de la table :\n{}", schema.prettyJson());
    }

    public void afficherNombreDeLignes() {
        long count = dataFrame.count();
        logger.info("Nombre de lignes dans le DataFrame : {}", count);
    }

    public void afficherContenu() {
        dataFrame.show();
        logger.info("Contenu du DataFrame affiché.");
    }

    public void fermerSession() {
        if (sparkSession != null) {
            sparkSession.close();
            logger.info("Session Spark fermée.");
        }
    }

    public String compute(String reqKey) {
        // Charger la requête SQL depuis les propriétés
        String sqlQuery = BigDataCC.props.getProperty(reqKey);
        if (sqlQuery == null) {
            throw new IllegalArgumentException("La clé " + reqKey + " est introuvable dans le fichier properties.");
        }

        // Exécuter la requête
        Dataset<Row> result = sparkSession.sql(sqlQuery);

        // Publier la table résultante en mémoire
        String resultTableName = reqKey + "_result";
        result.createOrReplaceTempView(resultTableName);

        return resultTableName;
    }
}
