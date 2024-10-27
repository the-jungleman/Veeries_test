import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

class VolDayProduct:
    def extract_table(self, url, harbor_name):
        try:
            # Disable warnings for insecure requests
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            res = requests.get(url, verify=False)
            soup = BeautifulSoup(res.content, 'html.parser')

            print(f"HTML content fetched from {url[:50]}...")
            tabs = soup.find_all('table')
            print(f"Found {len(tabs)} tables on {url}")
            data = []
            headers = [th.text.strip() for th in tab.find_all('th')]
               
            for tab_index, tab in enumerate(tabs):
                # Iterate through rows in the table with progress indication
                for r in tqdm(tab.find_all('tr')[1:], desc=f"Processing rows in table {tab_index + 1}"):
                    columns = r.find_all('td')
                    # len_columns = len(columns)

                    row_data = {header: columns[i].text.strip() for i, header in enumerate(headers) if i < len(columns)}
                    if harbor_name == "Santos" and len_columns > 9:
                        product = columns[8].text.strip()
                        volume_str = columns[9].text.strip().replace('.', '')
                        sentido = columns[7].text.strip()

                    elif harbor_name == "Paranaguá":
                        title = tab.find_previous('h2')
                        if 'Produto' in row_data and 'Sentido' in row_data:
                            product = row_data['Produto']
                            sentido = row_data['Sentido']
                        elif    'Prancha (t/dia)'   in  row_data    or  'Previsto':
                            volume_str = row_data['Volume'].replace('.', '')
                        else:
                            continue

                    else:
                        continue

                    # Handle volume conversion
                    try:
                        volume = int(volume_str) if volume_str.isdigit() else 0
                    except ValueError:
                        print(f"Invalid volume value: '{volume_str}' found in {url}")
                        volume = 0

                    # Append data if product and sentido are valid
                    if product and sentido:
                        data.append({
                            "Produto": product,
                            "Sentido": sentido,
                            "Volume": volume,
                            "Porto": harbor_name
                        })

            return data

        except Exception as e:
            print(f"Error while extracting data from {url}: {str(e)}")
            return []
            
if __name__ == "__main__":
    url_paranagua = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
    url_santos = "https://www.portodesantos.com.br/informacoes-operacionais/operacoesportuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/"
    
    vol_day_product = VolDayProduct()

    # Extract data from both harbors
    data_paranagua = vol_day_product.extract_table(url_paranagua, "Paranaguá")
    data_santos = vol_day_product.extract_table(url_santos, "Santos")

    # Combine and create DataFrame
    all_data = data_paranagua + data_santos
    df = pd.DataFrame(all_data)

    print(f"Total de linhas extraídas: {len(df)}")
    print(df)
    df.to_csv("volume_diario_ports_semagp.csv", index=False)
    
    # Group by Porto, Produto, and Sentido, and sum volumes
    resultado = df.groupby(['Porto', 'Produto', 'Sentido']).agg({'Volume': 'sum'}).reset_index()
    
    print(f"\nAgrupamento e soma de volumes:\n{resultado}")
    
    # Export the grouped results to a CSV file
    resultado.to_csv("volume_diario_ports.csv", index=False)
    print("Dados exportados com sucesso para volume_diario_ports.csv")