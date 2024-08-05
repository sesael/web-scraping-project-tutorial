from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd

url = "https://ycharts.com/companies/TSLA/revenues"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


# Retrieving the tables from the website
html = requests.get(url, "html", headers=headers)
soup = BeautifulSoup(html.content, "html.parser")
tables = soup.find_all("table")

# Converting the retrieved tables into a nested list
values = []
for table in tables:
    sub_values  = []
    for tr in table.find_all("tr"):
        val = []
        for td in tr.find_all("td"):
            val.append(td.get_text(separator="\n", strip=True))
        sub_values.append(val)
    values.append(sub_values)

# Concatenating the first and second tables 
# Deleting null values 
# Changing all values to billions and converting them to float type instead of strings
df = pd.concat([pd.DataFrame(values[0][1:], columns=["Date","Value (billions)"]),pd.DataFrame(values[1][1:], columns=["Date","Value (billions)"])]).reset_index(drop=True)
df["Value (billions)"] = [float(i[0:-1]) if i[-1] == "B" else (float(i[0:-1])/1000) for i in df["Value (billions)"]]
benchmarks = pd.DataFrame(values[2])
related_metrics = pd.DataFrame(values[3])


#Storing dataframe in SQL->

con = sqlite3.connect("Historical Revenue (Quarterly) Data.db")
cursor = con.cursor()
sql_table = '''
        CREATE TABLE IF NOT EXISTS revenues (
            Date TEXT,
            Value_in_billions FLOAT
        )
        '''
cursor.execute(sql_table)
df.to_sql(name = "revenues", con = con, if_exists = "replace", index = False)
con.commit()
df_example = pd.read_sql_query("SELECT * FROM revenues", con = con)
con.close()
