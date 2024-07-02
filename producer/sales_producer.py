import argparse
import atexit
import json
import logging
import random
import time
import sys
import os
import threading
from kafka import KafkaProducer

# Configuration du logging pour afficher les messages de log dans un fichier et sur la console
logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("./logs/sales_producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()

# Liste des vendeurs
with open('./villes.txt', 'r') as file:
    ZONES = [line.strip() for line in file]

# Fonction de callback pour gérer les succès d'envoi de message
def on_send_success(record_metadata):
    logger.info('Produit un enregistrement au topic {} partition [{}] @ offset {}'.format(
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset
    ))

# Fonction de callback pour gérer les erreurs d'envoi de message
def on_send_error(excp):
    logger.error('Erreur lors de la production de l\'enregistrement', exc_info=excp)

def produce_sales(args, thread_id):
    logger.info(f'Démarrage du producteur de ventes dans le thread {thread_id}')
    
    # Configuration du producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=200,
        client_id=f'sales-{thread_id}',
    )

    # S'assurer que tous les messages sont envoyés avant de quitter le programme
    atexit.register(lambda p: p.flush(), producer)

    i = 1
    while True:
        # Vérifier si c'est le dixième message
        is_tenth = i % 10 == 0

        # Générer des données de vente aléatoires
        sales = {
            'zone_id': random.choice(ZONES),
            'montant_usd': random.randrange(100, 1000),
            'vente_ts': int(time.time() * 1000) # horodatage de la vente en millisecondes
        }

        # Envoyer le message au topic Kafka
        future = producer.send(args.topic, value=sales)
        future.add_callback(on_send_success)  # Ajouter callback en cas de succès
        future.add_errback(on_send_error)    # Ajouter callback en cas d'erreur

        # Si c'est le dixième message, flusher les messages et attendre 1 seconde
        if is_tenth:
            producer.flush()
            time.sleep(1)
            i = 0  # Réinitialiser le compteur

        i += 1

def main(args):
    threads = []
    for thread_id in range(args.num_threads):
        thread = threading.Thread(target=produce_sales, args=(args, thread_id))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    # Analyse des arguments de la ligne de commande
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--num-threads', type=int, default=3, help='Nombre de threads à lancer')
    args = parser.parse_args()
    
    # Appel de la fonction principale avec les arguments analysés
    main(args)
