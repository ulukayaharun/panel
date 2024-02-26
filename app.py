from flask import Flask, request, render_template
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import urlparse
from datetime import datetime
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


app = Flask(__name__)
app.secret_key = "cokgizlisifre"
#veritabani bağlantısı
engine = create_engine("mysql+pymysql://username:password@ip_address/database_name",
                        connect_args={"charset": "utf8mb4"}, echo=False)

class AddingUrl:

    def __init__(self):
        self.link_df = pd.DataFrame(columns=["URL", "DATETIME"])  # Linkleri kaydetmek için DataFrame

    def add_link(self, link):
        timestamp = datetime.now()
        self.link_df.loc[len(self.link_df)] = [link, timestamp]
        self.save_to_database("url")

    def save_to_database(self, table_name):
        try:
            self.link_df.to_sql(table_name, engine, if_exists="append", index=False)
            self.link_df = pd.DataFrame(columns=["URL", "DATETIME"])  # DataFrame'i sıfırlar
        except Exception as e:
            print(e)

class WordFrequencies:

    def gsc_auth(scopes):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        #kullanıcıdan girilen verileri alır
        service = build('searchconsole', 'v1', credentials=creds)
        start_date=request.form.get("start_date")
        end_date=request.form.get("end_date")
        row_limit=request.form.get("row_limit")
        platform=request.form.get("platform")
        
        requests={
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["page"],
            "type": "discover",
            "rowLimit": row_limit
            }
        gsc_search_analytics = service.searchanalytics().query(siteUrl=f'sc-domain:{platform}',body=requests).execute()
        report = pd.DataFrame(data=gsc_search_analytics['rows'])

        report.to_sql('data',engine,if_exists="replace",index=False)

    def calculate_word_frequencies(engine, n: int):
        data = pd.read_sql_table("data", engine) 
        dict_address = {}
        for address in data["keys"]:
            address = str(address).split("/")[-1].split("-") #Keşfetten çekilen adresin
            #son son kısmını alır.
            for word in address:
                if len(word)>=4 and not word.isdigit():
                    if word in dict_address:
                        dict_address[word]+=1
                    else:
                        dict_address[word]=1

        #Büyükten küçüğe en çok tekrar eden "n" kelime olan listeye dönüşür.
        sorted_list = sorted(dict_address.items(), key=lambda t: t[1], reverse=True)[:n]
        df_address = pd.DataFrame(data=sorted_list, columns=["Kelimeler", "Tekrar Sayilari"])
        df_address.to_sql("word_frequencies", engine, if_exists="replace", index=False)
        return df_address

class NewsCounter:
    #https://www.example.com.tr/blabla ---> www.example.com.tr dönüştürücü
    def get_domain(url):
        parsed_url = urlparse(url)
        return parsed_url.netloc

    
    def update_df():
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")

        if start_date and end_date:
            # Formdan alınan tarihleri datetime türüne dönüştür ve saat bilgisini ekle
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

            # Veritabanı sorgusu için bitiş tarihine bir gün ekle (veya günün son saatini kullan)
            end_datetime = end_datetime.replace(hour=23, minute=59, second=59)

            df = pd.read_sql_table("sitemap_urls", engine)

            # Veritabanındaki 'DATE' sütununu datetime türüne çevir
            df['DATE'] = pd.to_datetime(df['DATE'])

            # Filtreleme işlemini güncellenmiş datetime aralığıyla yap
            filtered_df = df[(df['DATE'] >= start_datetime) & (df['DATE'] <= end_datetime)]

            data = {}
            for _, row in filtered_df.iterrows():
                domain = NewsCounter.get_domain(row["URL"])
                if domain in data:
                    data[domain] += 1
                else:
                    data[domain] = 1
            #büyükten küçüğe sıralar ve listeye dönüşür
            sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)

            new_df = pd.DataFrame(sorted_data, columns=['Domain', 'Haber Sayilari'])
            return new_df.to_html(index=False)
        

adding_url = AddingUrl()
word_frequencies = WordFrequencies()

#Anasayfa
@app.route("/")
def homepage():
    return render_template("homepage.html")

#StatusChecker Sayfası
@app.route("/addurltodatabase", methods=["GET", "POST"])
def add_url_to_database():

    if request.method == "POST":
        link = request.form.get("link")
        if link:
            adding_url.add_link(link)

    return render_template("addurltodatabase.html")

#Keşfet Sayfası
@app.route("/wordfrequenties", methods=["GET", "POST"])
def find_most_frequent_word():
    table_html = ""
    if request.method == "POST":
        n = request.form.get("n", type=int)
        df_address = WordFrequencies.calculate_word_frequencies(engine, n)
        table_html = df_address.to_html(index=False)
    return render_template("wordfrequenties.html", table_html=table_html)

#Haber Sayisi Sayfasi
@app.route("/countnews", methods=["GET", "POST"])
def make_table():
    table_html = ""
    if request.method == "POST":
        table_html = NewsCounter.update_df()
    return render_template("newscounter.html", table_html=table_html)

if __name__ == "__main__":
    app.run(debug=True)
