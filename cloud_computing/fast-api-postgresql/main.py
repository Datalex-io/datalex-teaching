import os
from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Charge les variables définies dans le fichier .env
load_dotenv()

app = FastAPI(title="DVD Rental API")

# Récupère la chaîne de connexion de manière sécurisée
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Crée et retourne une connexion à la base de données."""
    if not DATABASE_URL:
        print("Erreur : La variable DATABASE_URL n'est pas définie dans le fichier .env.")
        return None
        
    try:
        # RealDictCursor permet de récupérer les résultats sous forme de dictionnaires JSON-friendly
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        return None

@app.get("/getfilms")
def get_films(limit: int = 50):
    """Récupère une liste de films (limité à 50 par défaut pour éviter un retour trop lourd)."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Impossible de se connecter à la base de données")
    
    cur = conn.cursor()
    # On récupère quelques infos basiques sur les films
    cur.execute("SELECT film_id, title, description, release_year FROM film LIMIT %s;", (limit,))
    films = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {"films": films}

@app.get("/getcustomer/{customer_id}")
def get_customer_rentals(customer_id: int):
    """Récupère la liste des films loués par un client spécifique."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Impossible de se connecter à la base de données")
    
    cur = conn.cursor()
    
    # Requête avec jointures pour lier le client au film via l'inventaire et la location
    query = """
        SELECT f.title, r.rental_date, r.return_date
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.customer_id = %s
        ORDER BY r.rental_date DESC;
    """
    
    cur.execute(query, (customer_id,))
    rentals = cur.fetchall()
    
    cur.close()
    conn.close()
    
    if not rentals:
        raise HTTPException(status_code=404, detail="Client introuvable ou aucune location à son actif")
        
    return {
        "customer_id": customer_id, 
        "total_rentals": len(rentals),
        "rentals": rentals
    }