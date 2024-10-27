import  datetime,urllib3
from    bs4 import  BeautifulSoup
import  pandas  as  pd
import  selenium    import webdriver
from    selenium.webdriver.chrome.service   import  Service as  ChromeService
from    selenium.webdriver.firefox.service  import  Service as  FirefoxService
from    selenium.webdriver.edge.service import  Service as  EdgeService
from    selenium.common.exceptions  import  WebDriverException
from    webdriver_manager.chrome    import  ChromeDriverManager
from    webdriver_manager.firefox   import  GeckoDriverManager
from    webdriver_manager.microsoft import  EdgeChromiumDriverManager

class   Vol_day_product:
    def get_webdriver(self):
        try:
            options=webdriver.ChromeOptions()
            options.add_argument("--headless")
            return webdriver.Chrome(options=options, service=ChromeService(ChromeDriverManager().install()))
        except WebDriverException:
            try:
                options=webdriver.FirefoxOptions()
                options.add_argument("--headless")
                return webdriver.Firefox(options=options, service=FirefoxService(GeckoDriverManager().install()))
            except WebDriverException:
                try:
                    options=webdriver.EdgeOptions()
                    options.add_argument("--headless")
                    return webdriver.Edge(options=options, service=EdgeService(EdgeChromiumDriverManager().install()))
                except WebDriverException:
                    print("Nao foi possivel inicializar o webdriver")
                    return None

    def extract_table(self,url,harbor_name):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r=requests.get(url,verify=False)
        driver=self.get_webdriver()
        driver.get(url)

        if  r.status_code==200:
            try:
                soup=BeautifulSoup(r.content,'html.parser')
                driver.quit()
                tab=soup.find('table')
                if  tab is  None:
                    print("Tabela nao encontrada na pagina")
                    return  pd.DataFrame(columns=[
                        "Data","Produto", "Sentido", "Volume"
                        ])

                self.data=[]
                for r in tab.find_all('tr'):
                    columns=r.find_all('td')
                    if len(columns) == 4:
                        row=[col.text.strip() for col in columns]
                        row.appand(harbor_name)
                        self.data.append(row)

                print("Dados extraidos:",self.data)
                df_paranagua=pd.DataFrame(self.data,columns=[
                        "Data","Produto","Sentido","Volume","Porto"
                    ])

                return  df_paranagua

            except Exception as e:
                print(f"Error: {str(e)}")
                return  pd.DataFrame(columns=[
                        "Data","Produto","Sentido","Volume","Porto"
                    ])
                
        else:
            print(f"Erro ao acessar a URL: {url}")
            return pd.DataFrame(columns=[
                "Data","Produto", "Sentido", "Volume"
                ])
                

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

if __name__=="__main__":
    url_paranagua="https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
    url_santos="https://www.portodesantos.com.br/informacoes-operacionais/operacoesportuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/"
    
    vol_day_product=Vol_day_product()

    data_paranagua=vol_day_product.extract_table(url_paranagua,"Paranagua")
    data_santos=vol_day_product.extract_table(url_santos,"Santos")

    vol_day=vol_day_product.con_data(data_paranagua, data_santos)
    vol_day_product.export_csv(vol_day, "volume_diario_ports.csv")
