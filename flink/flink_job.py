import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # Créer un environnement de streaming
    env = StreamExecutionEnvironment.get_execution_environment()

    # Configurer les settings de l'environnement pour le streaming
    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .build()

    # Créer l'environnement de table
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # Ajouter la dépendance du connecteur Kafka
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            './jars/flink-sql-connector-kafka-3.2.0-1.19.jar')

    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))

    #######################################################################
    # Créer une table source Kafka avec DDL
    #######################################################################
    src_ddl = """
    CREATE TABLE sales_usd (
        zone_id VARCHAR,
        montant_usd DOUBLE,
        vente_ts BIGINT,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sales-usd',
        'properties.bootstrap.servers' = 'host.docker.internal:9092',
        'properties.group.id' = 'sales-usd',
        'properties.auto.offset.reset' = 'earliest',
        'format' = 'json'
    )
    """
    tbl_env.execute_sql(src_ddl)

    # Créer et initier le chargement de la table source
    tbl = tbl_env.from_path('sales_usd')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Charger les informations des villes depuis le fichier CSV
    #####################################################################
    csv_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'villes_cote_divoire.csv')

    villes_ddl = f"""
    CREATE TABLE villes_info (
        Ville STRING,
        Latitude FLOAT,
        Longitude FLOAT,
        Population BIGINT,
        Region STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{csv_file_path}',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true',
        'csv.ignore-first-line' = 'true'
    )
    """
    tbl_env.execute_sql(villes_ddl)

    villes_tbl = tbl_env.from_path('villes_info')

    print('\nVilles Schema')
    villes_tbl.print_schema()

    #####################################################################
    # Définir le calcul d'agrégation de fenêtre tumbling (Ventes par minute par vendeur)
    # en joignant les données des ventes avec les informations des villes
    #####################################################################
    sql = """
        SELECT
          sales_usd.zone_id,
          TUMBLE_END(sales_usd.proctime, INTERVAL '60' SECONDS) AS fenetre,
          SUM(sales_usd.montant_usd) * 0.85 AS vente_fenetre,
          villes_info.Latitude,
          villes_info.Longitude,
          villes_info.Population,
          villes_info.Region
        FROM sales_usd
        LEFT JOIN villes_info
        ON sales_usd.zone_id = villes_info.Ville
        GROUP BY
          TUMBLE(sales_usd.proctime, INTERVAL '60' SECONDS),
          sales_usd.zone_id,
          villes_info.Latitude,
          villes_info.Longitude,
          villes_info.Population,
          villes_info.Region
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Créer une table de destination Kafka
    ###############################################################
    sink_ddl = """
        CREATE TABLE sales_euros (
            zone_id VARCHAR,
            fenetre TIMESTAMP(3),
            vente_fenetre DOUBLE,
            Latitude FLOAT,
            Longitude FLOAT,
            Population BIGINT,
            Region VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'host.docker.internal:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # Écrire les agrégations de fenêtres temporelles dans la table de destination
    revenue_tbl.execute_insert('sales_euros').wait()

    tbl_env.execute('vente_par_minute_par_vendeur-euros')

if __name__ == '__main__':
    main()
