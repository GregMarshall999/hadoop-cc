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

    private Dataset<Row> dataFrame;

    /**
     * Charge un fichier dans un Spark DataSet
     * Etablie le DataFrame a partir du fichier CSV
     * @param sparkSession Session spark pour le traitement de Data
     * @param url adresse (physique) du fichier à charger
     * @param tableName Nom de la DataFrame
     */
    public EnedisDataSet(SparkSession sparkSession, String url, String tableName) {
        this.sparkSession = sparkSession;

        this.dataFrame = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(url);
                //.withColumnRenamed("week", "semaine"); //tentative de correction de la colonne week

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
        Dataset<Row> result = sparkSession.sql(sqlQuery); //potentiel erreur a throw

        // Publier la table résultante en mémoire
        String resultTableName = reqKey + "_result";
        result.createOrReplaceTempView(resultTableName);

        return resultTableName;
    }
}
