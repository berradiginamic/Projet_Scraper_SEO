from datetime import datetime
from urllib.parse import urljoin
import pymongo
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from logs.logs import log_error, log_event

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://localhost:27017/')
database = client['projetseo']


# Insertion du document dans la collection
def insert_url(coll, url, scope, status):
    new_url = {
        'url': url,
        'scope': scope,
        'status': status
    }
    try:
        pending_urls_correct = coll.insert_one(new_url)
        if pending_urls_correct.inserted_id:
            print(f"Document inséré avec succès. ID: {pending_urls_correct.inserted_id}")
        else:
            print("Échec de l'insertion du document.")
    except pymongo.errors.DuplicateKeyError:
        print("URL déjà existante dans la base de données.")
    except Exception as e:
        print(f"Une erreur s'est produite lors de l'insertion : {e}")


def get_pending_url(db):
    new_url = db.find_one_and_update({"status": "pending"},
                                     {"$set": {"status": "processing"}},
                                     return_document=pymongo.ReturnDocument.BEFORE
                                     )
    return new_url


def set_url_completed(db, url):
    # Marque l'URL comme traitée dans la base de données
    db.update_one({"_id": url["_id"]}, {"$set": {"status": "completed"}})


def simple_scrape(db, url, max_urls=10):  # On choisit 10 pour un retour rapide pour debugger
    response = requests.get(url['url'])

    # Vérifier si la requête a réussi (statut 200)
    if response.status_code == 200:
        # Utiliser BeautifulSoup pour analyser le contenu HTML de la page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extraire la balise <title>
        title_tag = soup.title.text.strip() if soup.title else None

        # Extraire les balises <h1>, <h2>
        header_tags = [header.text.strip() for header in soup.find_all(['h1', 'h2'])]

        # Extraire les balises <b>, <em>
        bold_tags = [bold.text.strip() for bold in soup.find_all('b')]
        italic_tags = [italic.text.strip() for italic in soup.find_all('em')]

        # Extraire les liens (balises <a>)
        link_tags = soup.find_all('a')
        links = [link.get('href') for link in link_tags if link.get('href')]

        # Extraire les liens (balises <a>) et les ajouter à la collection 'pending_urls'
        link_tags = soup.find_all('a')
        new_links = [urljoin(url['url'], link.get('href')) for link in link_tags if link.get('href')]

        # On place un compte pour garder le nombre de url à scraper:
        scraped_urls_count = 0

        for new_link in new_links:
            # Check if the link is not already in the 'pending_urls' collection
            if new_link.startswith(url['scope']) and not db['urls'].find_one({"url": new_link}):
                # Add the link to the 'pending_urls' collection
                db['urls'].insert_one({"url": new_link,"scope": url['scope'], "status": "pending"})

                # Increment le compte
            scraped_urls_count += 1

            # Check if the maximum limit is reached
            if scraped_urls_count >= max_urls:
                break


        # Stocker les informations dans MongoDB
        document_metadata = {
            "url": url['url'], "html": response.text,
            "title": title_tag,
            "header_tags": header_tags,
            "scrapping_date": datetime.now(),
            "bold_tags": bold_tags,
            "italic_tags": italic_tags,
        }

        # Insert the scraped document into the 'pages' collection
        db['pages_metadata'].insert_one(document_metadata)
        log_event(f"URL {url} scraped successfully.")
        print("Informations extraites et stockées dans la base de données.")
    else:
        log_error(get_pending_url(db), f"Failed to retrieve page. Status code: {response.status_code}")
        print(f"Échec de la récupération de la page. Code d'état : {response.status_code}")


