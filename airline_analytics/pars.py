pars
import pandas as pd
import requests #библеотека для запросов к серверу 
from bs4 import BeautifulSoup #библиотека для автоматического парсинга странички

URL = 'https://code.s3.yandex.net/learning-materials/data-analyst/festival_news/index.html'
req = requests.get(URL)
soup = BeautifulSoup(req.text, 'lxml')

table = soup.find('table',attrs={"id": "best_festivals"})

content=[]
for row in table.find_all('tr'):
    if not row.find_all('th'):
        content.append([element.text for element in row.find_all('td')])
        
festivals = (
    pd.DataFrame(content, columns = ["Название фестиваля", "Место проведения", "Дата проведения"])
)
print(festivals)
#['festival_name', 'festival_city', 'festival_date']