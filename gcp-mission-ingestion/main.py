"""
Entry point dell'applicazione FastAPI per il servizio di missione.

Contiene:
- Endpoint GET /health per il controllo di salute del servizio.
- Endpoint POST /mission per ricevere, arricchire e restituire i dati di missione.
"""

from datetime import datetime
from typing import Any, Dict

from fastapi import FastAPI

from models import MissionRequest
from utils.weather import get_weather

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
async def create_mission(mission: MissionRequest) -> Dict[str, Any]:
    """Riceve i dati di missione, li arricchisce con meteo e restituisce il record completo.

    Workflow
    --------
    1. Riceve il payload JSON e lo valida tramite il modello Pydantic MissionRequest.
    2. Chiama in modo asincrono la funzione get_weather() per ottenere i dati meteo
       relativi alle coordinate della missione.
    3. Crea un dizionario completo che contiene:
       - i dati della missione (mission_data)
       - i dati meteo (weather_data)
       - un campo "ingestion_timestamp" con datetime.utcnow().isoformat()
    4. Restituisce questo dizionario come risposta JSON.

    Parameters
    ----------
    mission : MissionRequest
        Dati della missione inviati dal client in formato JSON.

    Returns
    -------
    Dict[str, Any]
        Dizionario completo con missione, meteo e timestamp di ingestion.
    """
    # 1) Convertiamo il modello Pydantic in un dizionario Python standard.
    mission_data: Dict[str, Any] = mission.dict()

    # 2) Chiamata asincrona al servizio meteo, usando le coordinate della missione.
    weather_data: Dict[str, Any] = await get_weather(
        latitude=mission.latitude,
        longitude=mission.longitude,
    )

    # 3) Creiamo il record completo che rappresenta il documento finale da salvare/loggare.
    #    Manteniamo mission e weather come sotto-dizionari per chiarezza.
    record: Dict[str, Any] = {
        "mission": mission_data,
        "weather": weather_data,
        "ingestion_timestamp": datetime.utcnow().isoformat(),
    }

    # 4) Restituiamo il record completo come risposta JSON.
    return record


# Blocco eseguibile solo quando lanciamo il file direttamente con `python main.py`
# In ambiente reale (es. Docker/Cloud Run) verrà usato `uvicorn main:app`.
if __name__ == "__main__":
    import uvicorn

    # Avvio del server di sviluppo in locale.
    uvicorn.run(app, host="0.0.0.0", port=8080)

