"""
Definizione dei modelli Pydantic per l'API di missione.

Contiene il modello MissionRequest utilizzato per validare i dati
in ingresso sull'endpoint /mission.
"""

from typing import Literal

from pydantic import BaseModel, Field, validator, field_validator


class MissionRequest(BaseModel):
    """Modello di richiesta per la creazione di una missione.

    Attributes
    ----------
    mission_id : str
        Identificativo univoco della missione (es. "MISS-2024-001").
    vehicle_type : str
        Tipo di veicolo utilizzato per la missione. Valori ammessi:
        "drone", "helicopter", "ultralight".
    latitude : float
        Latitudine dell'area operativa, compresa tra -90 e 90.
    longitude : float
        Longitudine dell'area operativa, compresa tra -180 e 180.
    altitude_m : int
        Altitudine prevista in metri (valore strettamente positivo).
    pilot_id : str
        Identificativo del pilota responsabile della missione.
    """

    mission_id: str = Field(
        ...,
        description="Identificativo univoco della missione (es. MISS-2024-001).",
    )
    vehicle_type: Literal["drone", "helicopter", "ultralight"] = Field(
        ...,
        description='Tipo veicolo: "drone", "helicopter" o "ultralight".',
    )
    latitude: float = Field(
        ...,
        description="Latitudine dell'area operativa (range: -90, 90).",
    )
    longitude: float = Field(
        ...,
        description="Longitudine dell'area operativa (range: -180, 180).",
    )
    altitude_m: int = Field(
        ...,
        description="Altitudine prevista in metri (valore > 0).",
    )
    pilot_id: str = Field(
        ...,
        description="Identificativo del pilota responsabile.",
    )

    @field_validator("latitude")
    def validate_latitude(cls, value: float) -> float:
        """Valida che la latitudine sia compresa tra -90 e 90 gradi."""
        if not -90.0 <= value <= 90.0:
            raise ValueError("latitude must be between -90 and 90 degrees")
        return value

    @field_validator("longitude")
    def validate_longitude(cls, value: float) -> float:
        """Valida che la longitudine sia compresa tra -180 e 180 gradi."""
        if not -180.0 <= value <= 180.0:
            raise ValueError("longitude must be between -180 and 180 degrees")
        return value

    @field_validator("altitude_m")
    def validate_altitude(cls, value: int) -> int:
        """Valida che l'altitudine sia un intero strettamente positivo."""
        if value <= 0:
            raise ValueError("altitude_m must be a positive integer")
        return value
