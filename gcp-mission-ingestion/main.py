"""
Entry point dell'applicazione FastAPI per il servizio di missione.

Contiene:
- Endpoint GET /health per il controllo di salute del servizio.
- Endpoint POST /mission per ricevere e validare i dati di missione.
"""

from datetime import datetime
from typing import Any, Dict

from fastapi import FastAPI

from models import MissionRequest

# Creazione dell'istanza principale di FastAPI.
# Questo oggetto rappresenta la nostra applicazione web.
app = FastAPI(
    title="Mission Ingestion Service",
    description="Servizio di ingestion missioni e meteo (GCP Ingestion Layer).",
    version="1.0.0",
)


@app.get("/health", response_model=Dict[str, str])
def health_check() -> Dict[str, str]:
    """Restituisce lo stato di salute del servizio.

    Returns
    -------
    Dict[str, str]
        Un dizionario contenente:
        - "status": stato statico del servizio ("ok").
        - "timestamp": data/ora corrente in formato ISO 8601 (UTC).
    """
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/mission")
def create_mission(mission: MissionRequest) -> Dict[str, Any]:
    """Crea una nuova missione a partire dai dati ricevuti.

    Questo endpoint rappresenta il punto di ingresso principale per
    l'ingestion delle missioni. In questa versione iniziale si limita a
    validare il payload e restituire una conferma con i dati ricevuti.

    Parameters
    ----------
    mission : MissionRequest
        Dati della missione inviati dal client in formato JSON.

    Returns
    -------
    Dict[str, Any]
        Dizionario contenente un messaggio di conferma, un timestamp
        di ricezione e i dati della missione validati.
    """
    # Convertiamo il modello Pydantic in un dizionario Python standard.
    mission_data: Dict[str, Any] = mission.dict()

    return {
        "message": "Mission received successfully",
        "received_at": datetime.utcnow().isoformat(),
        "mission": mission_data,
    }


# Blocco eseguibile solo quando lanciamo il file direttamente con `python main.py`
# In ambiente reale (es. Docker/Cloud Run) verrà usato `uvicorn main:app`.
if __name__ == "__main__":
    import uvicorn

    # Avvio del server di sviluppo in locale.
    uvicorn.run(app, host="0.0.0.0", port=8080)
