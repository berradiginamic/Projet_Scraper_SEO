import time
from requests.exceptions import RequestException
from multiprocessing import Process
from database.connection_bdd import url_en_attente, collection
from logs.logs import log_error, log_event
from scraper.programme_scraper import database
from scraper.scraper_simple import get_pending_url, simple_scrape, set_url_completed

def distributed_scraper(base_url, process_id):
    max_retries = 5
    retry_delay = 60  # seconds

    while True:
        # Récupère une URL en attente de traitement depuis la base de données
        url_a_traiter = get_pending_url(database['urls'])

        if url_a_traiter:
            # Vérifie si l'URL n'a pas été traitée ou n'est pas dans la collection principale
            if not collection.find_one({"url": url_a_traiter["url"]}):
                retries = 0

                while retries < max_retries:
                    try:
                        # Traite l'URL
                        simple_scrape(collection, url_a_traiter)
                        # Marque l'URL comme terminée dans 'pending_urls'
                        set_url_completed(database['urls'], url_a_traiter)
                        break  # Sort de la boucle de réessai si réussi
                    except RequestException as e:
                        # Gère les exceptions liées à la requête (par exemple, problèmes de réseau, délais d'attente)
                        log_error(url_a_traiter, f"Error processing URL: {e}")
                        retries += 1
                        time.sleep(retry_delay)   # Introduit un délai avant de réessayer
                        log_event(f"Retrying... Attempt {retries}/{max_retries}")
                    except Exception as e:
                        # Handle other exceptions
                        log_error(url_a_traiter, f"Unexpected error: {e}")
                        # Mark the URL as completed to avoid repeated attempts
                        set_url_completed(database['urls'], url_a_traiter)
                        break  # Break out of the retry loop

                if retries == max_retries:
                    # If maximum retries reached, mark the URL as completed
                    log_event(f"Maximum retries reached for URL: {url_a_traiter}")
                    set_url_completed(database['urls'], url_a_traiter)
        else:
            # Si aucune URL en attente n'est trouvée, sort de la boucle
            break

if __name__ == "__main__":
    # Example: Run the distributed scraper as a separate process
    process = Process(target=distributed_scraper, args=('https://quotes.toscrape.com', 1))
    process.start()
    process.join()
