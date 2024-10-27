import  requests
import  urllib3
from    bs4 import  BeautifulSoup
import  pandas  as  pd

class   VolDayProduct:
    def extract_table(self,url,harbor_name):
        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            res=requests.get(url, verify=False)
            soup=BeautifulSoup(res.content, 'html.parser')

            tabs=soup.find_all('table')
            data=[]
            for tab in  tabs:
                for r   in  tab.find_all('tr')[1:]:  
                    columns=r.find_all('td')
                    if  len(columns)>9: 
                        date=columns[4].text.strip()
                        product=columns[8].text.strip()
                        volume_str=columns[9].text.strip().replace('.','')

                        try:
                            volume=int(volume_str)  if  volume_str.isdigit()    else    0
                        except  ValueError:
                            print(f"Valor de volume inválido: '{volume_str}' encontrado em {url}")
                            volume=0

                        sentido=columns[3].text.strip()
                        data.append({
                            "Data": date,
                            "Produto":  product,
                            "Sentido":  sentido,
                            "Volume":   volume,
                            "Porto":    harbor_name
                        })
            return  data

        except  Exception   as  e:
            print(f"Error while extracting data from {url}: {str(e)}")
            return[]
            
if __name__ == "__main__":
    url_paranagua = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
    url_santos = "https://www.portodesantos.com.br/informacoes-operacionais/operacoesportuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/"
    
    vol_day_product = VolDayProduct()

    data_paranagua = vol_day_product.extract_table(url_paranagua, "Paranaguá")
    data_santos = vol_day_product.extract_table(url_santos, "Santos")

    all_data = data_paranagua + data_santos
    df = pd.DataFrame(all_data)

    resultado = df.groupby(['Data', 'Porto', 'Produto', 'Sentido']).agg({'Volume': 'sum'}).reset_index()

    print(resultado)

    resultado.to_csv("volume_diario_ports.csv", index=False)
    print("Dados exportados com sucesso para volume_diario_ports.csv")