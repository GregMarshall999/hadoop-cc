package fr.dawan.paris;

import fr.dawan.paris.big.data.cc.EnedisDataSet;
import fr.dawan.paris.big.data.cc.FileDownloader;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class BigDataCC {
    private static final Logger logger = LoggerFactory.getLogger(BigDataCC.class);

    public static final Properties props = new Properties();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("EnedisDataSetApp")
                .setMaster("local[*]");

        try (SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate()) {
            logger.info("Session Spark initialisée.");

            //Fichier CSV
            String datasetUrl = "https://www.data.gouv.fr/fr/datasets/r/eedd0b42-6152-4c94-92ba-1d9a7bc8fd91";
            String localPath = "enedis_dataset.csv";
            FileDownloader.downloadFile(datasetUrl, localPath);

            EnedisDataSet enedisDataSet = new EnedisDataSet(sparkSession, localPath, "enedis_table");
            enedisDataSet.afficherSchema();
            enedisDataSet.afficherContenu();
            enedisDataSet.afficherNombreDeLignes();

            //Chargement SQL
            props.load(new FileInputStream("src/main/resources/database.properties"));

            EnedisDataSet sqlEnedisDataSet = new EnedisDataSet(sparkSession, "enedis_dataset.csv", "enedis_table");
            String[] queryKeys = {"residential_query", "professional_query", "enterprise_query"};

            for (String queryKey : queryKeys) {
                String resultTableName = sqlEnedisDataSet.compute(queryKey);
                sparkSession.sql("SELECT * FROM " + resultTableName).show();
            }

        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de l'application.", e);
        }
    }
}
