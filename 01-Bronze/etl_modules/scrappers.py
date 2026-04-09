import json
import logging
from datetime import datetime

import requests
import urllib3

from etl_modules.config import (
    COTO_HEADERS,
    COTO_PARAMS,
    REQUEST_TIMEOUT_SECONDS,
    SSL_VERIFY_FALLBACK,
    SOURCE_NAME,
)


# Avoid noisy SSL warnings when fallback verify=False is used.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def coto_scraping(url: str, corte: str):
    """Scrape one Coto product URL and return a normalized record dict."""
    try:
        try:
            # Preferred HTTPS request with certificate verification enabled.
            response = requests.get(url, headers=COTO_HEADERS, params=COTO_PARAMS, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.exceptions.SSLError as exc:
            # Fallback for environments where CA chain cannot be validated.
            logging.warning("SSL error en %s: %s", url, exc)
            response = requests.get(
                url,
                headers=COTO_HEADERS,
                params=COTO_PARAMS,
                timeout=REQUEST_TIMEOUT_SECONDS,
                verify=SSL_VERIFY_FALLBACK,
            )

        # Parse nested payload returned by Coto endpoint.
        data = response.json()
        datadto_price_str = data["contents"][0]["Main"][0]["record"]["attributes"]["sku.dtoPrice"][0]
        dto_price_json = json.loads(datadto_price_str)

        precio = dto_price_json["precioLista"]
        nombre = data["contents"][0]["Main"][0]["record"]["attributes"]["product.displayName"][0]

        return {
            "fecha_extraccion": datetime.now().strftime("%d-%m-%Y"),
            "supermercado": SOURCE_NAME,
            "nombre": corte,
            "descripcion": nombre,
            "precio": precio,
            "url": url,
        }
    except Exception:
        # Return None so caller can record URL-level failure without stopping the batch.
        logging.exception("Error con %s", url)
        return None


def run_coto_scraping(urls: dict):
    """Run Coto scraping over configured URLs and collect successes/errors."""
    resultados = []
    errores_scraping = []

    for corte in urls.keys():
        tiendas = urls[corte]["urls"]
        nombre = urls[corte]["nombre"]
        for supermercado in tiendas.keys():
            url_actual = tiendas[supermercado]

            try:
                info = coto_scraping(url_actual, nombre)
                if info:
                    resultados.append(info)
                else:
                    # Explicitly track URLs that returned no record.
                    errores_scraping.append(
                        {
                            "corte": nombre,
                            "supermercado": supermercado,
                            "url": url_actual,
                            "error_type": "NoData",
                            "error_message": "No se pudo extraer información",
                        }
                    )
                    logging.warning("No se pudo extraer info para %s - %s", nombre, url_actual)
            except Exception as exc:
                # Defensive catch in case unexpected errors bypass coto_scraping.
                errores_scraping.append(
                    {
                        "corte": nombre,
                        "supermercado": supermercado,
                        "url": url_actual,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
                logging.exception("Fallo inesperado en %s - %s", nombre, url_actual)

    return resultados, errores_scraping