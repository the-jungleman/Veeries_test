import  requests,datetime
from bs4 import BeautifulSoup
import  pandas  as  pd

class   Vol_day_product:
    def extract_paranagua(self,url):
        r=requests.get(url)
        if  r.status_code==200:
            try:
                soup=BeautifulSoup(r.content,'html.parser')
                tab=soup.find('table')
                data=[]
                for l in tab.find_all('tr'):
                    columns=l.find_all('td')
                    for col in columns:
                        data.append(col.text.strip())
                df_paranagua=pd.DataFrame(data,columns=[
                        "Data","Produto", "Sentido", "Volume"
                    ])
                return  df_paranagua
            except Exception as e:
                print(f"Error: {str(e)}")
        else:
            print(f"Erro ao acessar a URL: {url}")
            return df_paranagua

    def con_data(self,data_paranagua,data_santos):
        data_con=pd.concat([data_paranagua,data_santos],ignore_index=True)
        data_con=data_con[(data_con['Porto']=='Paranagua') |
                          (data_con['Porto']=='Santos')]
        vol_day=data_con.groupby(
            ['Data','Produto','Sentido']
            ).agg({'Volume':'sum'}).reset_index()
        return  vol_day

    def export_csv(self,df,file_name):
       df.to_csv(file_name,index=False)
       print(f"Dados exportados com sucesso para {file_name}")

if  __name__=="__main__":
    url_paranagua="https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
    url_santos = "https://www.portodesantos.com.br/informacoes-operacionais/operacoesportuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/"
    
    vol_day_product=Vol_day_product()
    
    data_paranagua=vol_day_product.extract_paranagua(url_paranagua)
    data_santos=vol_day_product.extract_paranagua(url_santos)

    vol_day=vol_day_product.con_data(data_paranagua,data_santos)
    vol_day_product.export_csv(vol_day,"volume_diario_ports.csv")
