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
        seller_id VARCHAR,
        amount_usd DOUBLE,
        sale_ts BIGINT,
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
    # Définir le calcul d'agrégation de fenêtre tumbling (Ventes par minute par vendeur)
    #####################################################################
    sql = """
        SELECT
          seller_id,
          TUMBLE_END(proctime, INTERVAL '60' SECONDS) AS window_end,
          SUM(amount_usd) * 0.85 AS window_sales
        FROM sales_usd
        GROUP BY
          TUMBLE(proctime, INTERVAL '60' SECONDS),
          seller_id
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Créer une table de destination Kafka
    ###############################################################
    sink_ddl = """
        CREATE TABLE sales_euros (
            seller_id VARCHAR,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
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

    tbl_env.execute('windowed-sales-euros')


if __name__ == '__main__':
    main()
