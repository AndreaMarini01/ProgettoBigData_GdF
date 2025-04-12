# Parsing
import pandas as pd
import numpy as np
import os
import sys
import pdfplumber
import xml.etree.ElementTree as ET
import fitz 
import re
import camelot
import warnings
import tabula
# Database Connection
import mysql.connector
from mysql.connector import Error
# Similarity
from Levenshtein import distance as levenshtein_distance
from datetime import datetime
# Utils
import tkinter as tk
from tkinter import filedialog

# Funzione per ottenere il path del file selezionato
def get_file_path():
    root = tk.Tk()
    root.withdraw()  
    file_path = filedialog.askopenfilename()
    return file_path

def parse_pdf_to_dataframe_man(file_path):

    # Estrazione con stream e parametri ottimizzati
    tables_stream = camelot.read_pdf(
        file_path,
        pages='all',
        flavor='stream',
        strip_text='\n',
        edge_tol=500,
        row_tol=10,
        column_tol=10,
        split_text=True
    )

    # Aggiunta di un dizionario per il mapping degli header
    HEADER_MAPPING = {
        'TargaReg. Number': 'Targa',
        'Tipo VeicoloVehicle Type': 'Veicolo',
        'Descrizione della merceDescription of Goods': 'Merce',
        'PesoWeight': 'Peso',
        'TaraTare': 'Tara',
        'CaricatoreShipper': 'Mittente',
        'RicevitoreConsignee': 'Destinatario'
    }

    # Lista per contenere tutti i dataframe puliti
    all_dfs = []

    for i, table in enumerate(tables_stream):
        df = table.df
        
        # Identificazione degli header cercando la riga più appropriata
        header_scores = []
        for idx in range(min(5, len(df))):
            non_empty_cells = df.iloc[idx].str.strip().str.len() > 0
            score = non_empty_cells.sum() + (df.iloc[idx].str.isupper().sum() * 0.5)
            header_scores.append(score)
        
        header_row = header_scores.index(max(header_scores))
        headers = df.iloc[header_row].str.strip()
        mapped_headers = headers.map(lambda x: HEADER_MAPPING.get(x, x))
        df.columns = mapped_headers
        
        # Pulizia dati
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        df = df.replace(r'^\s*$', '', regex=True)
        df = df.fillna('')
        df = df.dropna(how='all')
        df = df.drop_duplicates()
        
        all_dfs.append(df)

    # Unione di tutti i dataframe
    final_df = pd.concat(all_dfs, ignore_index=True)
    return final_df


def parse_pdf_to_dataframe_driver(file_path):
    data = []
    # Pattern principale per catturare la maggior parte delle righe
    pattern = re.compile(r"(\d+)\s+([A-Z0-9\-]+)\s+([A-Z0-9\-]+(?:\s+[A-Z0-9\-]+)?)\s+([A-Za-z]+)\s+([A-Za-z]+)?\s+([A-Z0-9]+)\s+([A-Z0-9\(\)\s\-]+)")

    with pdfplumber.open(file_path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            lines = text.split('\n')
            for line_number, line in enumerate(lines, start=1):
                match = pattern.match(line)
                if match:
                    sistemazione = " ".join(filter(None, [match.group(7)]))
                    entry = {
                        "N.ro CT": match.group(1),
                        "Biglietto": match.group(2),
                        "Targa": match.group(3),
                        "Cognome": match.group(4),
                        "Nome": match.group(5) if match.group(5) else "",  # Se manca Nome, si lascia vuoto
                        "Documento": match.group(6),
                        "Sistemazione": match.group(7).strip()
                    }
                    data.append(entry)
                else:
                    split_line = line.split()
                    if len(split_line) >= 6 and split_line[0].isdigit() and split_line[1].isdigit():
                        entry = {
                            "N.ro CT": split_line[0],
                            "Biglietto": split_line[1],
                            "Targa": split_line[2],                # Colonna 'Targa'
                            "Cognome": split_line[3],              # Colonna 'Cognome'
                            "Nome": "",                            # Colonna 'Nome' lasciata vuota
                            "Documento": split_line[4],            # Colonna 'Documento'
                            "Sistemazione": " ".join(split_line[5:]) # Colonna 'Sistemazione'
                        }
                        data.append(entry)

    # Conversionde dei dati in DataFrame
    df = pd.DataFrame(data)
    # Rimozione delle righe dove la colonna 'Nome' è vuota
    df = df[df["Nome"] != ""]

    return df

# Funzione per eseguire il parsing dei dati
def unifiedParsing(file_path):
    dataframes = []
    file_data=[]
    pdf_tables = []
    pd.set_option('display.max_rows', 10000)
    pd.set_option('display.max_columns', 10000) 
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', 100000)
    if file_path.endswith(('.csv')):
        header_mapping = {
            'Biglietto': 'Biglietto',
            'Sbarco': 'Sbarco',
            'In tansito': 'In transito',
            'Presentato': 'Presentato',
            'Cognome': 'Cognome',
            'Nome': 'Nome',
            'Nato il': 'Data nascita',
            'Nato a': 'luogo di nascita',
            'Nazionalita': "nazionalita'",
            'Documento': 'documento',
            'Veicolo': 'Veicolo',
            'Targa': 'Targa',
            'Note': 'Note'
        }
        df = pd.read_csv(file_path, encoding='latin-1', delimiter=";", escapechar=',', header=1, engine='python', on_bad_lines='skip')
        df.rename(columns=header_mapping, inplace=True)
        return df
    if file_path.endswith(('.xlsx')):
        df = pd.read_excel(file_path, header=0, engine='openpyxl')
        return df
    if file_path.endswith(('.xls')):
        df = pd.read_excel(file_path, header = 0, engine='xlrd')
        return df
    if file_path.endswith(('.xml')):
    # Dizionario per mappare gli header in italiano
        header_mapping = {
            'surname': 'cognome',
            'name': 'nome',
            'birthDate': 'data nascita',
            'bornPlace': 'luogo di nascita',
            'nationality': "nazionalita'",
            'boardingPort': 'porto partenza',
            'unboardingPort': 'porto arrivo',
            'identityDocumentType': 'tipo documento',
            'identityDocumentNumber': 'documento',
            'gender': 'sesso',
            'birthCountry': 'paese di nascita',
        }
        ns = {'ns': 'http://elsagdatamat.com/vts2/vts_pmis-v1'}
        tree = ET.parse(file_path)
        root = tree.getroot()
        for fal6data in root.findall(".//ns:fal6Data", namespaces=ns):
            tag_data = {
                'surname': fal6data.find('ns:surname', namespaces=ns).text if fal6data.find('ns:surname', namespaces=ns) is not None else '',
                'name': fal6data.find('ns:name', namespaces=ns).text if fal6data.find('ns:name', namespaces=ns) is not None else '',
                'birthDate': fal6data.find('ns:birthDate', namespaces=ns).text if fal6data.find('ns:birthDate', namespaces=ns) is not None else '',
                'bornPlace': fal6data.find('ns:bornPlace', namespaces=ns).text if fal6data.find('ns:bornPlace', namespaces=ns) is not None else '',
                'nationality': fal6data.find('ns:nationality', namespaces=ns).text if fal6data.find('ns:nationality', namespaces=ns) is not None else '',
                'boardingPort': fal6data.find('ns:boardingPort', namespaces=ns).text if fal6data.find('ns:boardingPort', namespaces=ns) is not None else '',
                'unboardingPort': fal6data.find('ns:unboardingPort', namespaces=ns).text if fal6data.find('ns:unboardingPort', namespaces=ns) is not None else '',
                'identityDocumentType': fal6data.find('ns:identityDocumentType', namespaces=ns).text if fal6data.find('ns:identityDocumentType', namespaces=ns) is not None else '',
                'identityDocumentNumber': fal6data.find('ns:identityDocumentNumber', namespaces=ns).text if fal6data.find('ns:identityDocumentNumber', namespaces=ns) is not None else '',
                'gender': fal6data.find('ns:gender', namespaces=ns).text if fal6data.find('ns:gender', namespaces=ns) is not None else '',
                'birthCountry': fal6data.find('ns:birthCountry', namespaces=ns).text if fal6data.find('ns:birthCountry', namespaces=ns) is not None else '',
            }
            file_data.append(tag_data)
            df = pd.DataFrame(file_data)
            df.rename(columns=header_mapping, inplace=True)
        return df
    if file_path.endswith(('.pdf')):
        return parse_pdf_to_dataframe_driver(file_path)

# Crea la connessione con il database
def crea_connessione():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',   
            database='gdf'
        )
        if conn.is_connected():
            print('Connesso al database')
            return conn
    except Error as e:
        print(f'Errore durante la connessione: {e}')
        return None

# Converte le date nel formato corretto
def converti_data(data):
    if isinstance(data, str):  # Controlla se è una stringa
        try:
            # Prova a convertire dal formato 'DD/MM/YYYY' a 'YYYY-MM-DD'
            return datetime.strptime(data, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return data  # Ritorna il valore originale se non è nel formato atteso
    return data  # Ritorna il valore originale se non è una stringa

def inserimento_tratta(connessione, porto_partenza, porto_arrivo, motonave):
    cursor = connessione.cursor()
    id_tratta = """
        SELECT id_tratta FROM tratte
        WHERE motonave = %s AND porto_partenza = %s AND porto_arrivo = %s 
        LIMIT 1
    """
    cursor.execute(id_tratta, (motonave, porto_partenza, porto_arrivo))
    risultato = cursor.fetchone()
    if risultato is None:
        nuova_tratta = """
            INSERT INTO tratte (motonave, porto, porto_partenza, porto_arrivo, link, tipo, archiviato)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        porto = input ("Inserisci il porto italiano di partenza o di arrivo")
        if porto == porto_partenza:
            tipo = "imbarco"
        elif porto == porto_arrivo:
            tipo = "sbarco"
        valori = (motonave, porto, porto_partenza, porto_arrivo, None, tipo, None)
        cursor.execute(nuova_tratta, valori)
        connessione.commit()
        id_tratta = cursor.lastrowid
        print("Nuova tratta inserita con successo")
    else:
        id_tratta = risultato[0]
        print("Tratta recuperata con successo")
    cursor.close()
    return id_tratta

def inserimento_manifesto(connessione, id_tratta):
    cursor = connessione.cursor()
    answer = input("Vuoi creare un nuovo manifesto? (y/n) ")
    if answer == "y":
        data_viaggio = input("Inserisci la data del viaggio (nel formato YYYY-MM-DD) ")
        data_viaggio = datetime.strptime(data_viaggio, "%Y-%m-%d").date()
        nuovo_manifesto = """
            INSERT INTO manifesti (id_tratta, data, ETA, data_inserimento, analisi_effettuata, note)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        valori = (id_tratta, data_viaggio, None, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), None, None)
        cursor.execute(nuovo_manifesto, valori)
        connessione.commit()
        id_manifesto = cursor.lastrowid
        print("Nuovo manifesto inserito con successo")
        print(f"ID del nuovo manifesto: {id_manifesto}")
    elif answer == "n":
        id_manifesto = input ("Inserisci l'id del manifesto di interesse ")
        manifesto = """
            SELECT id_manifesto FROM manifesti
            WHERE id_manifesto = %s
            LIMIT 1
        """
        cursor.execute(manifesto, (id_manifesto,))
        risultato = cursor.fetchone()
        if risultato is None:
            print("Nessun manifesto trovato con l'id specificato")
            return None
        else:
            id_manifesto = risultato[0]
            print("Manifesto recuperato con successo")
    cursor.close()
    return id_manifesto

def recupera_dati_passeggeri(connessione):
    cursor = connessione.cursor()
    cursor.execute('SELECT * FROM passeggeri')
    risultati = cursor.fetchall()
    header = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(risultati, columns=header)
    cursor.close()
    return df

def recupera_dati_veicoli(connessione):
    cursor = connessione.cursor()
    cursor.execute('SELECT * FROM veicoli')
    risultati = cursor.fetchall()
    header = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(risultati, columns=header)
    cursor.close()
    return df

def recupera_dati_motrici(connessione):
    cursor = connessione.cursor()
    cursor.execute('SELECT * FROM motrici')
    risultati = cursor.fetchall()
    header = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(risultati, columns=header)
    cursor.close()
    return df

def recupera_dati_rimorchi(connessione):
    cursor = connessione.cursor()
    cursor.execute('SELECT * FROM rimorchi')
    risultati = cursor.fetchall()
    header = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(risultati, columns=header)
    cursor.close()
    return df

def inserimento_passeggeri(connessione, file_path, id_manifesto):
    cursor = connessione.cursor()

    df_file = unifiedParsing(file_path)
    df_database = recupera_dati_passeggeri(connessione)

    # Converte gli header in minuscolo
    df_file.columns = df_file.columns.str.lower()
    df_database.columns = df_database.columns.str.lower()

    df_file['data nascita'] = df_file['data nascita'].apply(converti_data)

    # Elimina eventuali colonne Unnamed
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
    df_database = df_database.loc[:, ~df_database.columns.str.contains('^Unnamed')]

    # Elimina righe e colonne contenenti solamente valori null
    df_file = df_file.dropna(how='all', axis=1) # Colonne
    df_file = df_file.dropna(how='all', axis=0) # Righe

    # Sotituisce i NaN in None
    df_file = df_file.where(pd.notna(df_file), None)
    df_database = df_database.where(pd.notna(df_database), None)

    dizionario_passeggeri = {}
    # Iterazione su df_dataframe (db_index indice, db_row contenuto della riga)
    for db_index, db_row in df_database.iterrows():
        # Crea una chiave (key) formata dal nome e cognome
        key = (db_row['nome'].lower() if pd.notna(db_row['nome']) else None, 
            db_row['cognome'].lower() if pd.notna(db_row['cognome']) else None)
        # Salva la riga db_row in corrispondenza di key 
        dizionario_passeggeri[key] = db_row
    
    for index, row in df_file.iterrows():
        if not pd.notna(row.get('nome')) and not pd.notna(row.get('cognome')):
            continue
        key = (row['nome'].lower() if pd.notna(row['nome']) else None, 
            row['cognome'].lower() if pd.notna(row['cognome']) else None)
        # Cerca la key derivante da df_file all'interno del dizionario
        db_row = dizionario_passeggeri.get(key)
        
        if db_row is not None and not db_row.empty:
            # Corrispondenza esatta trovata
            print("Record trovato:", db_row['nome'], db_row['cognome'])
            # Controllo dell'esistenza del record in itempasseggeri
            id_passeggero = db_row['id_passeggero']
            
            query_check_itempasseggeri = """
                SELECT 1 FROM itempasseggeri 
                WHERE id_manifesto = %s AND id_passeggero = %s
                LIMIT 1
            """
            cursor.execute(query_check_itempasseggeri, (id_manifesto, id_passeggero))
            itempasseggero_esistente = cursor.fetchone()

            if itempasseggero_esistente:
                print(f"Il passeggero con id {id_passeggero} è già presente nel manifesto {id_manifesto}.")
            else:
                # Inserisci il record in itempasseggeri solo se non esiste
                query_itempasseggero = """
                    INSERT INTO itempasseggeri (id_manifesto, id_passeggero, biglietto, reference, sistemazione, note)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query_itempasseggero, (
                    id_manifesto,
                    id_passeggero,
                    None,
                    None,
                    None,
                    None
                ))
                connessione.commit()
        else:
            # Calcolo della similarità se non c'è corrispondenza esatta
            record_simile_trovato = False
            for db_key, db_value in dizionario_passeggeri.items():
                # Imposta una stringa vuota se `nome` o `cognome` è `None`
                nome_row = row['nome'] if pd.notna(row['nome']) else ''
                cognome_row = row['cognome'] if pd.notna(row['cognome']) else ''
                nome_db = db_value['nome'] if pd.notna(db_value['nome']) else ''
                cognome_db = db_value['cognome'] if pd.notna(db_value['cognome']) else ''

                similarity_nome = 1 - levenshtein_distance(nome_row.lower(), nome_db.lower()) / max(len(nome_db), len(nome_row))
                similarity_cognome = 1 - levenshtein_distance(cognome_row.lower(), cognome_db.lower()) / max(len(cognome_db), len(cognome_row))

                # Stampa tutte le similarità
                if similarity_nome > 0.7 and similarity_cognome > 0.7:
                    record_simile_trovato = True
                    print("Record simile trovato (database):", db_value['nome'], db_value['cognome'], db_value['documento'])
                    print("Record simile trovato (file):", row['nome'], row['cognome'], row['documento'])
                    # Controllo dell'esistenza del record in itempasseggeri
                    id_passeggero = db_value['id_passeggero']
                    documento_database = db_value["documento"]
                    documento_file = row["documento"]

                    # Controllo se il documento nel file è uguale a quello nel database
                    if documento_database == documento_file:
                        continue  # Salta al prossimo passeggero

                    # Inserimento di un nuovo passeggero quando il documento è diverso
                    query_passeggeri = """
                        INSERT INTO passeggeri (nominativo, nome, cognome, data_nascita, luogo_nascita, nazionalita, documento, ricontrollare)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values_passeggeri = (
                        row['nome'].upper() + ' ' + row['cognome'].upper() if pd.notna(row['nome']) and pd.notna(row['cognome']) else None, 
                        row['nome'].upper() if pd.notna(row['nome']) else None, 
                        row['cognome'].upper() if pd.notna(row['cognome']) else None, 
                        row['data nascita'] if pd.notna(row['data nascita']) else None,
                        row['luogo di nascita'] if pd.notna(row['luogo di nascita']) else None,
                        row["nazionalita'"] if pd.notna(row["nazionalita'"]) else None,
                        row["documento"].upper() if pd.notna(row["documento"]) else None, 
                        None
                    )
                    cursor.execute(query_passeggeri, values_passeggeri)
                    connessione.commit()
            
                    query_check_itempasseggeri = """
                        SELECT 1 FROM itempasseggeri 
                        WHERE id_manifesto = %s AND id_passeggero = %s
                        LIMIT 1
                    """
                    cursor.execute(query_check_itempasseggeri, (id_manifesto, id_passeggero))
                    itempasseggero_esistente = cursor.fetchone()

                    if itempasseggero_esistente:
                        print(f"Il passeggero con id {id_passeggero} è già presente nel manifesto {id_manifesto}.")
                    else:
                        # Inserimento del record in itempasseggeri solo se non esiste
                        query_itempasseggero = """
                            INSERT INTO itempasseggeri (id_manifesto, id_passeggero, biglietto, reference, sistemazione, note)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(query_itempasseggero, (
                            id_manifesto,
                            id_passeggero,
                            None,
                            None,
                            None,
                            None
                        ))
                        connessione.commit()
            # Se nessun record simile è stato trovato, inserisci il nuovo passeggero (modificato)
            if not record_simile_trovato:
                query_passeggeri = """
                    INSERT INTO passeggeri (nominativo, nome, cognome, data_nascita, luogo_nascita, nazionalita, documento, ricontrollare)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                if file_path.endswith('.xml'):
                    values_passeggeri = (
                        row['nome'].upper() + ' ' + row['cognome'].upper() if pd.notna(row['nome']) and pd.notna(row['cognome']) else None, 
                        row['nome'].upper() if pd.notna(row['nome']) else None, 
                        row['cognome'].upper() if pd.notna(row['cognome']) else None, 
                        row['data nascita'] if pd.notna(row['data nascita']) else None,
                        row['luogo di nascita'] if pd.notna(row['luogo di nascita']) else None,
                        row["nazionalita'"] if pd.notna(row["nazionalita'"]) else None,
                        row["documento"] if pd.notna(row["documento"]) else None, 
                        None
                    )
                    cursor.execute(query_passeggeri, values_passeggeri)
                    connessione.commit()
                if file_path.endswith('.csv'):
                    # Controlla che 'nome' e 'cognome' non siano entrambi vuoti
                    if row.get('nome') == '' and row.get('cognome') == '':
                        continue

                    # Controllo per dividere 'cognome' in 'cognome' e 'nome' se necessario
                    if pd.notna(row.get('cognome')) and pd.isna(row.get('nome')):
                        parts = row['cognome'].split()  # Divide il valore in base agli spazi
                        if len(parts) > 1:
                            row['cognome'] = parts[0].upper()  # La prima parola è il cognome
                            row['nome'] = ' '.join(parts[1:]).upper()  # Le altre parole costituiscono il nome
                        else:
                            row['cognome'] = row['cognome'].upper()  # Se c'è solo una parola, è il cognome
                            row['nome'] = None  # Nome rimane vuoto

                    # Composizione del nominativo
                    nominativo = ''
                    if pd.notna(row['nome']):
                        nominativo += row['nome'].upper()
                    if pd.notna(row['cognome']):
                        nominativo += (' ' + row['cognome'].upper()) if nominativo else row['cognome'].upper()

                    # Controlla se il nominativo esiste già nel database
                    query_check = "SELECT id_passeggero FROM passeggeri WHERE nominativo = %s"
                    cursor.execute(query_check, (nominativo,))
                    db_row = cursor.fetchall()

                    # Salta l'inserimento se il documento è None e il nominativo è già presente
                    if db_row is not None and pd.isna(row.get("documento")):
                        continue

                    values_passeggeri = (
                        nominativo if nominativo else None,
                        row['nome'].upper() if pd.notna(row['nome']) else None,
                        row['cognome'].upper() if pd.notna(row['cognome']) else None,
                        row['data nascita'] if pd.notna(row['data nascita']) else None,
                        row['luogo di nascita'] if pd.notna(row['luogo di nascita']) else None,
                        row["nazionalita'"] if pd.notna(row["nazionalita'"]) else None,
                        row["documento"] if pd.notna(row["documento"]) else None,
                        None
                    )

                    cursor.execute(query_passeggeri, values_passeggeri)
                    connessione.commit()

                    # Ottiene l'id del passeggero appena inserito
                    id_passeggero = cursor.lastrowid

                    # Inserimento del record nella tabella itempasseggeri
                    query_itempasseggeri = """
                        INSERT INTO itempasseggeri (id_manifesto, id_passeggero, biglietto, reference, sistemazione, note)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    values_itempasseggeri = (
                        id_manifesto,
                        id_passeggero,
                        row.get('biglietto') if pd.notna(row.get('biglietto')) else None,  # Valore del biglietto
                        None,  # Reference
                        None,  # Sistemazione
                        None   # Note
                    )
                    cursor.execute(query_itempasseggeri, values_itempasseggeri)
                    connessione.commit()
                elif not file_path.endswith('.xml') and not file_path.endswith('.csv'):
                    values_passeggeri = (
                        row['nome'].upper() + ' ' + row['cognome'].upper() if pd.notna(row['nome']) and pd.notna(row['cognome']) else None, 
                        row['nome'].upper() if pd.notna(row['nome']) else None, 
                        row['cognome'].upper() if pd.notna(row['cognome']) else None, 
                        row['data nascita'] if pd.notna(row['data nascita']) else None,
                        row['luogo di nascita'] if pd.notna(row['luogo di nascita']) else None,
                        row["nazionalita'"] if pd.notna(row["nazionalita'"]) else None,
                        None, 
                        None
                    )
                    cursor.execute(query_passeggeri, values_passeggeri)
                    connessione.commit()

                # Ottiene l'id del passeggero inserito
                id_passeggero = cursor.lastrowid

                # Controlla se esiste già un record in `itempasseggeri` con lo stesso `id_manifesto` e `id_passeggero`
                query_check_itempasseggeri = """
                    SELECT 1 FROM itempasseggeri WHERE id_manifesto = %s AND id_passeggero = %s
                """
                cursor.execute(query_check_itempasseggeri, (id_manifesto, id_passeggero))
                itempasseggero_esistente = cursor.fetchone()
                cursor.fetchall()

                if not itempasseggero_esistente:
                    # Inserisci il record in `itempasseggeri` solo se non esiste
                    query_itempasseggero = """
                        INSERT INTO itempasseggeri (id_manifesto, id_passeggero, biglietto, reference, sistemazione, note)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    values_itempasseggero = (
                        id_manifesto,
                        id_passeggero,
                        None,
                        None,
                        None,
                        None
                    )
                    cursor.execute(query_itempasseggero, values_itempasseggero)
                    connessione.commit()
    cursor.close()

def inserimento_driver(connessione, file_path, id_manifesto):     
    cursor = connessione.cursor()
    df_file = unifiedParsing(file_path)
    df_database = recupera_dati_passeggeri(connessione)
    df_veicoli = recupera_dati_veicoli(connessione)

    # Converte gli header in minuscolo
    df_file.columns = df_file.columns.str.lower()
    df_database.columns = df_database.columns.str.lower()
    df_veicoli.columns = df_veicoli.columns.str.lower()

    # Elimina eventuali colonne Unnamed
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
    df_database = df_database.loc[:, ~df_database.columns.str.contains('^Unnamed')]
    df_veicoli = df_veicoli.loc[:, ~df_veicoli.columns.str.contains('^Unnamed')]

    # Elimina righe e colonne contenenti solamente valori null
    df_file = df_file.dropna(how='all', axis=1) # Colonne
    df_file = df_file.dropna(how='all', axis=0) # Righe
    df_veicoli = df_veicoli.dropna(how='all', axis=1) # Colonne
    df_veicoli = df_veicoli.dropna(how='all', axis=0) # Righe

    # Sotituisce i NaN in None
    df_file = df_file.where(pd.notna(df_file), None)
    df_database = df_database.where(pd.notna(df_database), None)
    df_veicoli = df_veicoli.where(pd.notna(df_veicoli), None)

    # Confronta nome e cognome tra df_file e df_database
    for index, row in df_file.iterrows():
        nome_file = row.get('nome')
        cognome_file = row.get('cognome')
        documento_file = row.get('documento')
        biglietto_file = row.get('biglietto')
        sistemazione_file = row.get('sistemazione')
        targa_file = row.get('targa')

        # Trova il passeggero corrispondente nel database
        match = df_database[
            (df_database['nome'] == nome_file) &
            (df_database['cognome'] == cognome_file)
        ]

        if not match.empty:
            # Aggiorna il documento solo se non è già uguale o è vuoto
            for _, db_row in match.iterrows():
                if db_row['documento'] != documento_file or db_row["documento"] is None:
                    query = """
                        UPDATE passeggeri
                        SET documento = %s
                        WHERE id_passeggero = %s
                    """
                    cursor.execute(query, (documento_file, db_row['id_passeggero']))
                    connessione.commit()
                # Aggiorna la tabella itempasseggeri (biglietto e sistemazione)
                query = """
                    UPDATE itempasseggeri
                    SET biglietto = %s, sistemazione = %s
                    WHERE id_passeggero = %s AND id_manifesto = %s
                """
                cursor.execute(query, (biglietto_file, sistemazione_file, db_row['id_passeggero'], id_manifesto))
                connessione.commit()
        # Aggiorna la tabella itemveicoli se la targa coincide
        veicolo_match = df_veicoli[df_veicoli['targa'] == targa_file]
        if not veicolo_match.empty:
            for _, veicolo_row in veicolo_match.iterrows():
                query = """
                    UPDATE itemveicoli
                    SET biglietto = %s
                    WHERE targa = %s AND id_manifesto = %s
                """
                cursor.execute(query, (biglietto_file, targa_file, id_manifesto))
                connessione.commit()

        # Estrai la parte della targa prima del "-"
        targa_file = row['targa'].split('-')[0].strip() if row['targa'] else None

        # Aggiorna il biglietto in itemcamion se la targa coincide
        if targa_file:
            query_aggiorna_biglietto = """
                UPDATE itemcamion
                SET biglietto = %s
                WHERE id_motrice = (
                    SELECT id_motrice
                    FROM motrici
                    WHERE SUBSTRING_INDEX(targa, '-', 1) = %s
                    LIMIT 1
                );
            """
            cursor.execute(query_aggiorna_biglietto, (row.get('biglietto'), targa_file))
            connessione.commit()

    popola_conduzionecamion(connessione)

    # Chiudi il cursore
    cursor.close()


def inserimento_veicoli(connessione, file_path, id_manifesto):
    cursor = connessione.cursor()

    df_file = unifiedParsing(file_path)
    df_database = recupera_dati_veicoli(connessione)

    # Converte gli header in minuscolo
    df_file.columns = df_file.columns.str.lower()
    df_database.columns = df_database.columns.str.lower()

    # Elimina eventuali colonne Unnamed
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
    df_database = df_database.loc[:, ~df_database.columns.str.contains('^Unnamed')]

    # Elimina righe e colonne contenenti solamente valori null
    df_file = df_file.dropna(how='all', axis=1) # Colonne
    df_file = df_file.dropna(how='all', axis=0) # Righe
    df_database = df_database.dropna(how='all', axis=1) # Colonne
    df_database = df_database.dropna(how='all', axis=0) # Righe

    # Sotituisce i NaN in None
    df_file = df_file.where(pd.notna(df_file), None)
    df_database = df_database.where(pd.notna(df_database), None)

    # Crea un dizionario per il confronto rapido dei record di `veicoli`
    dizionario_veicoli = {}
    for db_index, db_row in df_database.iterrows():
        targa_key = db_row['targa'].lower().strip() if pd.notna(db_row['targa']) else None
        dizionario_veicoli[targa_key] = db_row

    # Itera sui record dei veicoli nel file
    for index, row in df_file.iterrows():
        targa_key = row['targa'].lower().strip() if pd.notna(row['targa']) else None
        db_row = dizionario_veicoli.get(targa_key)

        if db_row is not None and not db_row.empty:
            # Il veicolo esiste già, quindi non inseriamo nulla in `veicoli`
            print("Veicolo già presente:", db_row['targa'], db_row['classe'])
            # Recupera l'id_veicolo dal record esistente
            id_veicolo = db_row['id_veicolo']
        else:
            # Il veicolo è nuovo, inseriscilo in `veicoli`
            query_veicoli = """
                INSERT INTO veicoli (targa, classe, modello, nazionalita, telepass, ricontrollare)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            values_veicoli = (
                row.get('targa').upper() if pd.notna(row.get('targa')) else None,
                row.get('tipo veicolo') if pd.notna(row.get('tipo veicolo')) else None, # Forse da sostituire con classe
                None,
                row.get("nazionalita'") if pd.notna(row.get("nazionalita'")) else None,
                None,
                None
            )
            cursor.execute(query_veicoli, values_veicoli)
            connessione.commit()
            # Ottiene l'id del nuovo veicolo appena inserito
            id_veicolo = cursor.lastrowid
        # Controlla se esiste già un record in `itemveicoli` con lo stesso `id_manifesto` e `id_veicolo`
        query_check_itemveicoli = """
            SELECT 1 FROM itemveicoli WHERE id_manifesto = %s AND id_veicolo = %s
        """
        cursor.execute(query_check_itemveicoli, (id_manifesto, id_veicolo))
        itemveicolo_esistente = cursor.fetchone()

        if itemveicolo_esistente:
            print(f"Il veicolo con id {id_veicolo} è già presente nel manifesto {id_manifesto}.")
        else:
            # Inserisci il record in `itemveicoli` solo se non esiste
            query_itemveicoli = """
                INSERT INTO itemveicoli (id_manifesto, id_veicolo, biglietto, reference, note)
                VALUES (%s, %s, %s, %s, %s)
            """
            values_itemveicoli = (
                id_manifesto,
                id_veicolo,
                None,
                None,
                None
            )
            cursor.execute(query_itemveicoli, values_itemveicoli)
            connessione.commit()
    
    popola_conduzioneveicoli(connessione = connessione, file_path = file_path, id_manifesto = id_manifesto) 
    cursor.close()


def inserimento_camion(connessione, file_path, id_manifesto):

    cursor = connessione.cursor()

    # Carica i dati dal file e dal database
    if file_path.endswith(('.pdf')):
        df_file = parse_pdf_to_dataframe_man(file_path)
    else:
        df_file = unifiedParsing(file_path)
    df_motrici = recupera_dati_motrici(connessione)
    df_rimorchi = recupera_dati_rimorchi(connessione)

    # Converte gli header in minuscolo
    df_file.columns = df_file.columns.str.lower()
    df_motrici.columns = df_motrici.columns.str.lower()
    df_rimorchi.columns = df_rimorchi.columns.str.lower()

    # Rimuove eventuali colonne 'Unnamed'
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
    df_motrici = df_motrici.loc[:, ~df_motrici.columns.str.contains('^Unnamed')]
    df_rimorchi = df_rimorchi.loc[:, ~df_rimorchi.columns.str.contains('^Unnamed')]

    # Elimina righe e colonne contenenti solamente valori null
    df_file = df_file.dropna(how='all', axis=1) # Colonne
    df_file = df_file.dropna(how='all', axis=0) # Righe
    df_motrici = df_motrici.dropna(how='all', axis=1) # Colonne
    df_motrici = df_motrici.dropna(how='all', axis=0) # Righe
    df_rimorchi = df_rimorchi.dropna(how='all', axis=1) # Colonne
    df_rimorchi = df_rimorchi.dropna(how='all', axis=0) # Righe

    # Sotituisce i NaN in None
    df_file = df_file.where(pd.notna(df_file), None)
    df_motrici = df_motrici.where(pd.notna(df_motrici), None)
    df_rimorchi = df_rimorchi.where(pd.notna(df_rimorchi), None)

    # Crea dizionari per motrici e rimorchi
    dizionario_motrici = {db_row['targa'].lower(): db_row for _, db_row in df_motrici.iterrows() if pd.notna(db_row['targa'])}
    dizionario_rimorchi = {db_row['targa'].lower(): db_row for _, db_row in df_rimorchi.iterrows() if pd.notna(db_row['targa'])}

     # Itera sui record nel file
    for index, row in df_file.iterrows():
        max_length = 10
        targa = row.get('targa')
        targa_motrice, targa_rimorchio = None, None

        if targa and '-' in targa:
            targa_motrice, targa_rimorchio = map(str.strip, targa.split('-', 1))
        else:
            targa_motrice = targa  # Considera tutto come motrice se non c'è '-'
        # Gestione della motrice
        id_motrice = None
        if targa_motrice:
            if len(targa_motrice) > max_length:
                print (f"Targa troppo lunga: {targa_motrice}. Non verrà inserita.")
            else:
                db_motrice = dizionario_motrici.get(targa_motrice.lower())
                if db_motrice is not None:
                    id_motrice = db_motrice['id_motrice']
                else:
                    query_motrici = """
                        INSERT INTO motrici (targa, nazionalita, telepass, ricontrollare)
                        VALUES (%s, %s, %s, %s)
                    """
                    values_motrici = (
                        targa_motrice.upper(),
                        None,
                        None,
                        None
                    )
                    cursor.execute(query_motrici, values_motrici)
                    connessione.commit()
                    id_motrice = cursor.lastrowid
                    dizionario_motrici[targa_motrice.lower()] = {'id_motrice': id_motrice, 'targa': targa_motrice}
       # Gestione del rimorchio
        id_rimorchio = None
        if targa_rimorchio:
            if len(targa_rimorchio) > max_length:
                print (f"Targa troppo lunga: {targa_rimorchio}. Non verrà inserita.")
            else:
                db_rimorchio = dizionario_rimorchi.get(targa_rimorchio.lower())
                if db_rimorchio is not None:
                    id_rimorchio = db_rimorchio['id_rimorchio']
                else:
                    query_rimorchi = """
                        INSERT INTO rimorchi (targa, nazionalita, telepass, ricontrollare)
                        VALUES (%s, %s, %s, %s)
                    """
                    values_rimorchi = (
                        targa_rimorchio.upper(),
                        None,
                        None,
                        None
                    )
                    cursor.execute(query_rimorchi, values_rimorchi)
                    connessione.commit()
                    id_rimorchio = cursor.lastrowid
                    dizionario_rimorchi[targa_rimorchio.lower()] = {'id_rimorchio': id_rimorchio, 'targa': targa_rimorchio}

        # Gestione delle ditte
        query_ditta = "SELECT id_ditta FROM ditte WHERE nome = %s"
        query_inserisci_ditta = "INSERT INTO ditte (nome, p_iva) VALUES (%s, %s)"
        # Mittente
        id_ditta_mittente = None
        nome_ditta_mittente = row.get('mittente', '')
        if nome_ditta_mittente:
            cursor.execute(query_ditta, (nome_ditta_mittente,))
            result_ditta_mittente = cursor.fetchone()
            if result_ditta_mittente:
                id_ditta_mittente = result_ditta_mittente[0]
            else:
                cursor.execute(query_inserisci_ditta, (nome_ditta_mittente, None))
                connessione.commit()
                id_ditta_mittente = cursor.lastrowid
        # Destinatario
        id_ditta_destinatario = None
        nome_ditta_destinatario = row.get('destinatario', '')
        if nome_ditta_destinatario:
            cursor.execute(query_ditta, (nome_ditta_destinatario,))
            result_ditta_destinatario = cursor.fetchone()
            if result_ditta_destinatario:
                id_ditta_destinatario = result_ditta_destinatario[0]
            else:
                cursor.execute(query_inserisci_ditta, (nome_ditta_destinatario, None))
                connessione.commit()
                id_ditta_destinatario = cursor.lastrowid
            
        # Controlla se esiste già un record in `itemcamion`
        query_check_itemcamion = """
            SELECT 1 FROM itemcamion WHERE id_manifesto = %s AND id_motrice = %s
        """
        cursor.execute(query_check_itemcamion, (id_manifesto, id_motrice))
        itemcamion_esistente = cursor.fetchone()

        if itemcamion_esistente:
            print(f"Il camion con id_motrice {id_motrice} e id_rimorchio {id_rimorchio} è già presente nel manifesto {id_manifesto}.")
        else:
            query_itemcamion = """
                INSERT INTO itemcamion (id_manifesto, biglietto, reference, merce, peso, id_motrice, id_rimorchio, id_ditta_mittente, id_ditta_destinatario, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            max_length_merce = 250
            merce = row.get('merce')
            peso = row.get('peso', None)
            if isinstance(peso,str):
                try:
                    peso = float(peso.replace('.', '').replace(',', '.'))
                except ValueError:
                    print(f"Valore di peso non valido: {peso}. Impostato a NULL.")
                    peso = None
            else:
                peso = None
            if merce and len(merce) > max_length_merce:
                merce = merce[:max_length_merce]  # Troncamento del valore
            if id_motrice is not None:
                values_itemcamion = (
                    id_manifesto,
                    None,
                    None,
                    merce,
                    peso,
                    id_motrice,
                    id_rimorchio,
                    id_ditta_mittente if id_ditta_mittente else None,
                    id_ditta_destinatario if id_ditta_destinatario else None,
                    None
                )
                values_itemcamion = tuple(value if value != '' else None for value in values_itemcamion)
                cursor.execute(query_itemcamion, values_itemcamion)
                connessione.commit()

def popola_conduzioneveicoli(connessione, file_path, id_manifesto):
    cursor = connessione.cursor()
    aggiornato = False

    df_file = unifiedParsing(file_path)
    df_file.columns = df_file.columns.str.lower()

    # Query per selezionare tutti i passeggeri con il loro id_manifesto, nome e cognome
    query_passeggeri = """
        SELECT ip.id_itempasseggero, ip.id_manifesto, p.nominativo
        FROM itempasseggeri ip
        JOIN passeggeri p ON ip.id_passeggero = p.id_passeggero
        WHERE ip.id_manifesto = %s
    """
    cursor.execute(query_passeggeri, (id_manifesto,))
    passeggeri = cursor.fetchall()  # Otteniamo una lista di tuple (id_itempasseggero, id_manifesto, biglietto)

    passeggeri_per_manifesto = {}
    for id_itempasseggero, id_manifesto_db, nominativo in passeggeri:
        if id_manifesto_db not in passeggeri_per_manifesto:
            passeggeri_per_manifesto[id_manifesto_db] = {}
        passeggeri_per_manifesto[id_manifesto_db][nominativo.strip().upper()] = id_itempasseggero
    
    # Query per ottenere tutti i veicoli
    query_veicoli = """
        SELECT iv.id_itemveicolo, iv.id_manifesto, v.targa
        FROM itemveicoli iv
        JOIN veicoli v ON iv.id_veicolo = v.id_veicolo
        WHERE iv.id_manifesto = %s
    """
    cursor.execute(query_veicoli, (id_manifesto,))
    veicoli = cursor.fetchall()

    # Organizza i veicoli per id_manifesto
    veicoli_per_manifesto = {}
    for id_itemveicolo, id_manifesto_db, targa in veicoli:
        if id_manifesto_db not in veicoli_per_manifesto:
            veicoli_per_manifesto[id_manifesto_db] = []
        veicoli_per_manifesto[id_manifesto_db].append({"id_itemveicolo": id_itemveicolo, "targa": targa.strip().upper()})

    for _, row in df_file.iterrows():
        conducente = row['conducente'].strip().upper()  # Nome completo del conducente
        conducente_inverso = " ".join(reversed(conducente.split()))  # Cognome Nome invertito
        targa_veicolo = row['targa'].strip().upper() if 'targa' in row else None

        if not conducente or not targa_veicolo:
            continue

        # Verificare se il conducente è presente tra i passeggeri del manifesto
        if id_manifesto in passeggeri_per_manifesto:
            passeggero_trovato = None
                
            for nominativo, id_itempasseggero in passeggeri_per_manifesto[id_manifesto].items():
                # Confronto basato su set delle parole del nominativo
                set_conducente = set(conducente.split())
                set_nominativo = set(nominativo.split())
                if set_conducente == set_nominativo:
                    passeggero_trovato = id_itempasseggero
                    break

            if passeggero_trovato:
                # Cercare il veicolo con la targa corrispondente
                veicolo_trovato = None
                if id_manifesto in veicoli_per_manifesto:
                    for veicolo in veicoli_per_manifesto[id_manifesto]:
                        if veicolo["targa"] == targa_veicolo:
                            veicolo_trovato = veicolo["id_itemveicolo"]
                            break

                if veicolo_trovato:
                    # Verifica se l'associazione esiste già
                    query_controllo = """
                        SELECT 1 FROM conduzioneveicoli 
                        WHERE id_itempasseggero = %s AND id_itemveicolo = %s
                    """
                    cursor.execute(query_controllo, (passeggero_trovato, veicolo_trovato))
                    relazione_esiste = cursor.fetchone()

                    # Se non esiste, inserisci l'associazione
                    if not relazione_esiste:
                        query_inserisci_conduzione = """
                            INSERT INTO conduzioneveicoli (id_itempasseggero, id_itemveicolo)
                            VALUES (%s, %s)
                        """
                        cursor.execute(query_inserisci_conduzione, (passeggero_trovato, veicolo_trovato))
                        aggiornato = True
                else:
                    print(f"Nessun veicolo trovato per targa {targa_veicolo} nel manifesto {id_manifesto}")
            else:
                print(f"Conducente {conducente} non trovato per manifesto {id_manifesto}")
        else:
            print(f"Nessun passeggero per manifesto {id_manifesto}")

    # Effettuare il commit delle modifiche
    connessione.commit()
    if aggiornato:
        print("Tabella conduzioneveicoli popolata con successo.")
    else:
        print("Nessuna nuova associazione tra passeggeri e veicoli.")
    cursor.close()

def popola_conduzionecamion(connessione):
    cursor = connessione.cursor()
    aggiornato = False

    # Query per selezionare tutti i passeggeri con il loro id_manifesto
    query_passeggeri = """
        SELECT id_itempasseggero, id_manifesto, biglietto
        FROM itempasseggeri
    """
    cursor.execute(query_passeggeri)
    passeggeri = cursor.fetchall()  # Otteniamo una lista di tuple (id_itempasseggero, id_manifesto, biglietto)

    # Query per selezionare tutti i camion con il loro id_manifesto
    query_camion = """
        SELECT id_itemcamion, id_manifesto, biglietto
        FROM itemcamion
    """
    cursor.execute(query_camion)
    camion = cursor.fetchall()  # Otteniamo una lista di tuple (id_itemcamion, id_manifesto, biglietto)

    # Organizza i camion per id_manifesto e biglietto (da qui)
    camion_per_manifesto = {}
    for id_itemcamion, id_manifesto, biglietto in camion:
        if id_manifesto not in camion_per_manifesto:
            camion_per_manifesto[id_manifesto] = {}
        camion_per_manifesto[id_manifesto][biglietto] = id_itemcamion

    # Associa ogni passeggero a un camion con lo stesso id_manifesto e biglietto
    for id_itempasseggero, id_manifesto_p, biglietto_p in passeggeri:
        # Verifica se esiste un camion con lo stesso id_manifesto e biglietto
        if biglietto_p is not None and id_manifesto_p in camion_per_manifesto and biglietto_p in camion_per_manifesto[id_manifesto_p]:
            id_itemcamion = camion_per_manifesto[id_manifesto_p][biglietto_p]

            # Controlla se la relazione esiste già
            query_controllo = """
                SELECT 1 FROM conduzionecamion 
                WHERE id_itempasseggero = %s AND id_itemcamion = %s
            """
            cursor.execute(query_controllo, (id_itempasseggero, id_itemcamion))
            relazione_esiste = cursor.fetchone()

            # Se la relazione non esiste, procedi con l'inserimento
            if not relazione_esiste:
                query_inserisci_conduzione = """
                    INSERT INTO conduzionecamion (id_itempasseggero, id_itemcamion)
                    VALUES (%s, %s)
                """
                cursor.execute(query_inserisci_conduzione, (id_itempasseggero, id_itemcamion))
                aggiornato = True

    connessione.commit()
    if aggiornato:
        print("Tabella conduzionecamion popolata con successo.")
    else:
        print("Nessuna nuova associazione tra passeggeri e camion.")

    # Chiudi il cursore
    cursor.close()

connessione = crea_connessione()
file_path = get_file_path()
file_name = os.path.basename(file_path)
file_name = os.path.splitext(file_name)[0] # Nome del file senza estensione
divisione = file_name.split()
id_tratta = inserimento_tratta(connessione, divisione[1], divisione[2], divisione[4])
id_manifesto = inserimento_manifesto(connessione, id_tratta)
if file_name.startswith("PAX"):
    inserimento_passeggeri(connessione=connessione, file_path=file_path, id_manifesto = id_manifesto)
if file_name.startswith("DRIVER"):
    inserimento_driver(connessione=connessione, file_path=file_path, id_manifesto= id_manifesto)
if file_name.startswith("CAR"):
    inserimento_veicoli(connessione=connessione, file_path=file_path, id_manifesto= id_manifesto)
elif file_name.startswith("MAN"):
    inserimento_camion(connessione=connessione, file_path=file_path, id_manifesto = id_manifesto)
connessione.close()