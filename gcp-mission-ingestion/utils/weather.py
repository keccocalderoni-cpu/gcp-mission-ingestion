"""
Modulo weather: integrazione con Open-Meteo per recupero dati meteo.

Contiene la funzione asincrona get_weather(), utilizzata dal servizio di ingestion
per arricchire i dati della missione con condizioni atmosferiche aggiornate.

La funzione esegue:
- chiamata HTTP asincrona con timeout
- gestione errori di rete e HTTP
- normalizzazione dei dati in un dizionario coerente
- nessuna API key richiesta (servizio Open-Meteo)
"""

import logging
from typing import Dict, Any

import httpx


# Configurazione base del logger per il modulo.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Endpoint pubblico Open-Meteo per dati meteo correnti
API_URL = "https://api.open-meteo.com/v1/forecast"


async def get_weather(latitude: float, longitude: float) -> Dict[str, Any]:
    """Recupera le condizioni meteo da Open-Meteo per le coordinate fornite.

    Parameters
    ----------
    latitude : float
        Latitudine del punto per cui recuperare i dati meteo.
    longitude : float
        Longitudine del punto di interesse.

    Returns
    -------
    Dict[str, Any]
        Un dizionario contenente:
        - "temperature": temperatura in gradi Celsius (float o None)
        - "wind_speed": velocità del vento in km/h (float o None)
        - "condition": breve descrizione testuale stimata

    Notes
    -----
    - Open-Meteo non richiede API key.
    - In caso di errore di rete o risposta non valida, viene effettuato logging
      dell'errore e viene ritornato un dizionario con valori di fallback.
    """

    # Parametri della richiesta GET come richiesto da Open-Meteo.
    # Usiamo il blocco "current" per avere i dati meteo correnti.
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": ["temperature_2m", "wind_speed_10m", "is_day"],
        "wind_speed_unit": "kmh",
        "timezone": "UTC",
    }

    # Timeout di 5 secondi come da specifica.
    timeout = httpx.Timeout(5.0, connect=5.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.get(API_URL, params=params)
            response.raise_for_status()

            data = response.json()

            current = data.get("current", {})
            temperature = current.get("temperature_2m")
            wind_speed = current.get("wind_speed_10m")

            # Open-Meteo non dà direttamente una "condition" testuale su questo endpoint.
            # Per scopi didattici stimiamo una condizione molto semplice.
            is_day = current.get("is_day")
            if temperature is None or wind_speed is None:
                condition = "unknown"
            elif temperature < 0:
                condition = "freezing"
            elif wind_speed > 40:
                condition = "windy"
            else:
                condition = "clear-day" if is_day else "clear-night"

            return {
                "temperature": temperature,
                "wind_speed": wind_speed,
                "condition": condition,
            }

        except httpx.HTTPError as exc:
            logger.error(f"HTTP error during weather fetch: {exc}")
            return {
                "temperature": None,
                "wind_speed": None,
                "condition": "unknown",
            }

        except Exception as exc:
            logger.error(f"Unexpected error in get_weather(): {exc}")
            return {
                "temperature": None,
                "wind_speed": None,
                "condition": "unknown",
            }
