"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import os
    import zipfile
    import pandas as pd

    os.getcwd()
    carpeta = "files/input"
    dataframes = []

    for archivo in os.listdir(carpeta):
        if archivo.endswith(".zip"):
            ruta_zip = os.path.join(carpeta, archivo)
            
            # Abrir el ZIP y leer cada CSV dentro
            with zipfile.ZipFile(ruta_zip) as z:
                for nombre in z.namelist():
                    if nombre.endswith(".csv"):
                        with z.open(nombre) as f:
                            df = pd.read_csv(f)
                            df["origen_zip"] = archivo  # fuente - origen del df
                            dataframes.append(df)

    # Combinar todos los DataFrames en uno solo
    df_final = pd.concat(dataframes, ignore_index=False)

    # Limpieza
    df_final["job"] = df_final["job"].str.replace(".", "", regex=False)
    df_final["job"] = df_final["job"].str.replace("-", "_", regex=False)
    df_final["education"] = df_final["education"].str.replace(".", "_", regex=False)
    df_final["education"] = df_final["education"].replace("unknown", pd.NA)
    df_final["education"] = df_final["education"].replace("unknown", pd.NA)
    df_final["credit_default"] = df_final["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    df_final["mortgage"] = df_final["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    df_final["previous_outcome"] = df_final["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    df_final["campaign_outcome"] = df_final["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    df_final["year"] = 2022
    month_dict = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
    df_final["month"] = df_final["month"].str.lower().map(month_dict)
    df_final["last_contact_date"] = pd.to_datetime(df_final[["year", "month", "day"]]).dt.strftime("%Y-%m-%d")

    # Creación de los 3 df
    df_client = df_final[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    df_campaign = df_final[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "last_contact_date"]]
    df_economics = df_final[["client_id", "cons_price_idx", "euribor_three_months"]]

    #Guardar los 3 df
    ruta_salida = "files/output"

    df_client.to_csv(os.path.join(ruta_salida,"client.csv"), index=False)
    df_campaign.to_csv(os.path.join(ruta_salida,"campaign.csv"), index=False)
    df_economics.to_csv(os.path.join(ruta_salida,"economics.csv"), index=False)






    return


if __name__ == "__main__":
    clean_campaign_data()
