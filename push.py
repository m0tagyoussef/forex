import sqlite3
import pandas as pd
import talib
import http.client, urllib
import schedule
import time
from pairs import majors

# send notifications to Pushover
def send_notification(message):
  conn = http.client.HTTPSConnection("api.pushover.net:443")
  conn.request("POST", "/1/messages.json",
    urllib.parse.urlencode({
      "token": "a3j4ds3s5gdw2r66cbeembgs7ng38q",
      "user": "udipsnpi8uospnuyku751pmmyq4j8r",
      "message": "hello world",
    }), { "Content-type": "application/x-www-form-urlencoded" })

  response = conn.getresponse()
  print(response.status, response.reason)


def do_analysis():
  con = sqlite3.connect("database.db", timeout=30, isolation_level=None)
  con.execute('pragma journal_mode=wal')

  for epic in majors:
    parmas = "CHART:" + epic
    df = pd.read_sql_query("SELECT * FROM data WHERE pair IS ?",
        con, params=(parmas,))
    print(df)
    print(epic)
    

#schedule.every().hour.at(":00").do(do_analysis)
schedule.every(3).seconds.do(do_analysis)

while True:
    schedule.run_pending()
    #time.sleep(1800)
    time.sleep(1)
