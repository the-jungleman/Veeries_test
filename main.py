import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

class VolDayProduct:
    def extract_table(self, url, harbor_name):
        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            res = requests.get(url, verify=False)
            soup = BeautifulSoup(res.content, 'html.parser')

            print(f"HTML content fetched from {url[:50]}...")
            tabs = soup.find_all('table')
            print(f"Found {len(tabs)} tables on {url}")

            data = []
            for tab_index, tab in enumerate(tabs):
                for r in tqdm(tab.find_all('tr')[1:], desc=f"Processing rows in table {tab_index + 1}"):
                    columns = r.find_all('td')

                    if harbor_name == "Santos" and len(columns) > 9:
                        product = columns[8].text.strip()
                        volume_str = columns[9].text.strip().replace('.', '')
                        sentido = columns[7].text.strip()

                    elif harbor_name == "Paranaguá" and len(columns) > 17:
                        title = tab.find_previous('h2')
                        product, volume_str, sentido = '', '', ''

                        if title:
                            match title.text:
                                case t if "ATRACADOS" in t:
                                    product = columns[13].text.strip()
                                    volume_str = columns[16].text.strip().replace('.', '')
                                    sentido = columns[9].text.strip()
                                
                                case t if "PROGRAMADOS" in t or "AO LARGO PARA REATRACAÇÃO" in t:
                                    product = columns[12].text.strip()
                                    volume_str = columns[17].text.strip().replace('.', '')
                                    sentido = columns[9].text.strip()
                                
                                case t if "AO LARGO" in t:
                                    product = columns[11].text.strip()
                                    volume_str = columns[16].text.strip().replace('.', '')
                                    sentido = columns[8].text.strip()
                                
                                case t if "ESPERADOS" in t:
                                    product = columns[11].text.strip()
                                    volume_str = columns[15].text.strip().replace('.', '')
                                    sentido = columns[8].text.strip()
                                
                                case t if "DESPACHADOS" in t:
                                    product = columns[12].text.strip()
                                    volume_str = columns[16].text.strip().replace('.', '')
                                    sentido = columns[9].text.strip()
                                
                                case t if "APOIO PORTUÁRIO / OUTROS" in t:
                                    continue

                    else:
                        continue

                    try:
                        volume = int(volume_str) if volume_str.isdigit() else 0
                    except ValueError:
                        print(f"Invalid volume value: '{volume_str}' found in {url}")
                        volume = 0

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

    data_paranagua = vol_day_product.extract_table(url_paranagua, "Paranaguá")
    data_santos = vol_day_product.extract_table(url_santos, "Santos")

    all_data = data_paranagua + data_santos
    df = pd.DataFrame(all_data)

    print(f"Total de linhas extraídas: {len(df)}")
    print(df)
    df.to_csv("volume_diario_ports_semagp.csv", index=False)
    
    resultado = df.groupby(['Porto', 'Produto', 'Sentido']).agg({'Volume': 'sum'}).reset_index()
    print(f"\nAgrupamento e soma de volumes:\n{resultado}")
    resultado.to_csv("volume_diario_ports.csv", index=False)
    print("Dados exportados com sucesso para volume_diario_ports.csv")
