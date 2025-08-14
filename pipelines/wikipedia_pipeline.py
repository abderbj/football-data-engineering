from datetime import datetime
import pandas as pd
import json

def get_wikipidia_page(url):
    import requests
    print (f"Fetching Wikipedia page: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()

        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching wikipedia page: {e}")
        return None
    

def get_wikipidia_data(html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try different table class combinations
    table_selectors = [
        'table.wikitable.sortable',
        'table.wikitable', 
        'table[class*="wikitable"]',
        'table.sortable',
        'table'
    ]
    
    table = None
    for selector in table_selectors:
        table = soup.select_one(selector)
        if table:
            print(f"Found table using selector: {selector}")
            break
    
    if table is None:
        print("No suitable table found. Available tables:")
        tables = soup.find_all('table')
        for i, t in enumerate(tables[:5]):  # Show first 5 tables
            classes = t.get('class', [])
            print(f"  Table {i+1}: classes = {classes}")
        return []
    
    table_rows = table.find_all('tr')
    print(f"Found {len(table_rows)} rows in the table")
    return table_rows
    

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    import pandas as pd
    url = kwargs['url']
    print(f"Starting extraction from URL: {url}")
    
    html = get_wikipidia_page(url)
    if html is None:
        raise Exception("Failed to fetch HTML content")
    
    rows = get_wikipidia_data(html)
    if not rows:
        raise Exception("No data rows found in the Wikipedia table")
    
    # Convert each row to a dict of cell values
    extracted = []
    for row in rows:
        cells = row.find_all(['th', 'td'])
        cell_text = [cell.get_text(strip=True) for cell in cells]
        if cell_text:
            extracted.append(cell_text)
    
    print(f"Successfully extracted {len(extracted)} rows from Wikipedia")
    
    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        if len(tds) < 7:
            continue
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)
        data_df = pd.DataFrame(data)
        data_df.to_csv("data/ouput.csv", index=False)
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    
    return "OK"

def get_lat_long(country, city):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.geocode(f"{city}, {country}")
    if location:
        return location.latitude, location.longitude
    return None

def transform_wikipedia_data(**kwargs):
    print("transform_wikipedia_data started")
    try:
        data = kwargs['ti'].xcom_pull(task_ids='extract_data_from_wikipedia', key='rows')
        data = json.loads(data)
        stadium_df = pd.DataFrame(data)
        stadium_df["images"] = stadium_df["images"].apply(lambda x: x if x not in [None, "NO_IMAGE", ''] else "NO_IMAGE")
        stadium_df['capacity'] = stadium_df['capacity'].astype(int)
        kwargs['ti'].xcom_push(key='rows', value=stadium_df.to_json())
        return "Ok"
    except Exception as e:
        print(f"Error in transform_wikipedia_data: {e}")
        raise

def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_wikipedia_data', key='rows')
    
    data = json.loads(data)
    data = pd.DataFrame(data)
    file_name =("stadium_cleaned_" + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(':', ',') + ".csv")
    data.to_csv('data/' + file_name, index=False)