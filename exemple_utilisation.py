from scraper.programme_scraper import insert_url, database, simple_scrape, get_pending_url, set_url_completed

# Veuil
insert_url(database["urls"], 'https://www.lelibrepenseur.org/categorie/llp-theque/conferences/', 'https://www.lelibrepenseur.org', 'pending')

# Une
while True:
    # Récupère une URL en attente de traitement depuis la base de données
    url_a_traiter = get_pending_url(database['urls'])

    if url_a_traiter:
        # Process the URL
        simple_scrape(database, url_a_traiter)
        # Mark the URL as completed in 'pending_urls'
        set_url_completed(database['urls'], url_a_traiter)
    else:
        # Si aucune URL en attente n'est trouvée, sort de la boucle
        break
