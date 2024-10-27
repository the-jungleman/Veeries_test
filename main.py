import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd

class VolDayProduct:
    def extract_table(self, url, harbor_name):
        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            res = requests.get(url, verify=False)
            soup = BeautifulSoup(res.content, 'html.parser')

            tabs = soup.find_all('table')
            data = []
            for tab in tabs:
                for r in tab.find_all('tr')[1:]:  
                    columns = r.find_all('td')
                    product = ''
                    sentido = ''
                    volume = 0 

                    if len(columns) > 9 and harbor_name == "Santos": 
                        product = columns[8].text.strip()
                        volume_str = columns[9].text.strip().replace('.', '')
                        sentido = columns[7].text.strip()

                        try:
                            volume = int(volume_str) if volume_str.isdigit() else 0
                        except ValueError:
                            print(f"Invalid volume value: '{volume_str}' found in {url}")
                            volume = 0


                    elif len(columns) > 17 and harbor_name == "Paranaguá": 
                        title = tab.find_previous('h2')
                        if title and "AO LARGO" not in title.text:
                            product = columns[12].text.strip()
                            volume_str = columns[17].text.strip().replace('.', '')
                            sentido = columns[9].text.strip()
                        elif title and "ESPERADOS" in title.text:
                            product = columns[10].text.strip()
                            volume_str = columns[14].text.strip().replace('.', '')
                            sentido = columns[8].text.strip()

                        elif title and "APOIO PORTUÁRIO / OUTROS" in title.text:
                            continue
                        else:
                            product = columns[11].text.strip()
                            volume_str = columns[16].text.strip().replace('.', '')
                            sentido = columns[8].text.strip()

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

    # Check if necessary columns exist before grouping
    if 'Porto' in df.columns and 'Produto' in df.columns and 'Sentido' in df.columns:
        resultado = df.groupby(['Porto', 'Produto', 'Sentido']).agg({'Volume': 'sum'}).reset_index()
        print(resultado)
        resultado.to_csv("volume_diario_ports.csv",index=False)
        print("Dados exportados com sucesso para volume_diario_ports.csv")
    else:
        print("Erro: DataFrame não contém as colunas necessárias para agrupamento.")