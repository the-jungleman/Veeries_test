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
            tabs = soup.find_all('table')
            # print(f"Found {len(tabs)} tables on {url}")
            data = []

            for tab in tabs:
                headers = [th.text.strip() for th in tab.find_all('th')]

                rows = tab.find_all('tr')
                for r in tqdm(rows[1:]):
                    columns = r.find_all('td')
                    if len(columns) < 1:
                        continue
                    
                    if harbor_name == "Santos" and len(columns) > 9:
                        product = columns[8].text.strip()
                        volume_str = columns[9].text.strip().replace('.', '')
                        sentido = columns[7].text.strip()
                        try:
                            volume = float(volume_str)
                        except ValueError:
                            volume = volume_str  # Manter string caso não seja possível converter

                        data.append({
                            "Porto": harbor_name,
                            "Mercadoria": product,
                            "Sentido": sentido,
                            "Volume": volume
                        })

                    if  harbor_name=="Paranaguá":
                        row_data = {header: columns[i].text.strip() for i, header in enumerate(headers) if i < len(columns)}

                        # Inicializa variáveis para armazenar os dados desejados
                        mercadoria = row_data.get('Operador', '')
                        sentido = row_data.get('Bordo', row_data.get('Bordo', ''))
                        previsao = row_data.get('Previsto', '')
                        prancha = row_data.get('Prancha (t/dia)', '')

                        # Certifica-se de que as colunas foram extraídas corretamente
                        if mercadoria and sentido and (previsao or prancha):
                            data.append({
                                "Porto": harbor_name,
                                "Mercadoria": mercadoria,
                                "Sentido": sentido,
                                "Volume": previsao or prancha,
                            })
                            # print(f"Data extracted: Porto={harbor_name}, Mercadoria={mercadoria}, Sentido={sentido}, Volume={previsao or prancha}")

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

    print(df)

    if not df.empty:
        df.to_csv("volume_diario_ports_semagp.csv", index=False)
        
        # Agrupamento dos dados para sumarizar o volume
        resultado = df.groupby(['Porto', 'Mercadoria', 'Sentido']).agg({'Volume': 'first'}).reset_index()
        
        print(f"\nAgrupamento de dados:\n{resultado}")
        
        resultado.to_csv("volume_diario_ports.csv", index=False)
        print("Dados exportados com sucesso para volume_diario_ports.csv")
    else:
        print("Nenhum dado para exportar.")
