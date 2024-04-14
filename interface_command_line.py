import argparse
import pymongo
from pymongo import MongoClient


def parse_args():
    parser = argparse.ArgumentParser(description='Insert URLs into MongoDB')
    parser.add_argument('--url', type=str, help='URL to insert')
    parser.add_argument('--scope', type=str, help='Scope of the URL')
    parser.add_argument('--status', type=str, default='pending', help='Status of the URL (default: pending)')
    return parser.parse_args()


def insert_url(db, url, scope, status):
    new_url = {
        'url': url,
        'scope': scope,
        'status': status
    }
    try:
        urls_to_scrape = db['pending_urls_correct'].insert_one(new_url)
        if urls_to_scrape.inserted_id:
            print(f"Document inséré avec succès. ID: {urls_to_scrape.inserted_id}")
        else:
            print("Échec de l'insertion du document.")
    except pymongo.errors.DuplicateKeyError:
        print("URL déjà existante dans la base de données.")
    except Exception as e:
        print(f"Une erreur s'est produite lors de l'insertion : {e}")


if __name__ == "__main__":
    args = parse_args()

    # Connexion à la base de données MongoDB
    try:
        client = MongoClient('mongodb://localhost:27017/')
        database = client['projetseo']
    except pymongo.errors.ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
        exit(1)

    # If arguments are provided, insert the URL into the database
    if args.url and args.scope:
        insert_url(database, args.url, args.scope, args.status)

    else:
        print("Veuillez fournir au moins les arguments --url et --scope.")
