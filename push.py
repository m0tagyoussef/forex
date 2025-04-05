import sqlite3
import pandas as pd
import pandas_ta as ta
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
      "message": message,
    }), { "Content-type": "application/x-www-form-urlencoded" })

  response = conn.getresponse()
  print(response.status, response.reason)
  
def check_signal(row):
    con = sqlite3.connect("database.db", timeout=30, isolation_level=None)
    con.execute('pragma journal_mode=wal')
    crsr = con.cursor()
    
    if row["p_type"] == "bid":
        if row ["RSI_14"] >= 70 and row["pushed"] == 0:
            message = f"Sell {row['pair']} at {row.name}"
            send_notification(message)
            
            crsr.execute("""
                UPDATE data
                SET pushed = 1,
                rsi = ?
                WHERE id = ?
            """, (row["RSI_14"],row["id"]))
            
            return True
            
    elif row["p_type"] == "offer":
        if row ["RSI_14"] <= 30 and row["pushed"] == 0:
            message = f"Buy {row['pair']} at {row.name}"
            #send_notification(message)
            
            crsr.execute("""
                UPDATE data
                SET pushed = 1,
                rsi = ?
                WHERE id = ?
            """, (row["RSI_14"],row["id"]))
            
            return True
    
    else:
        return False

def do_analysis():
  con = sqlite3.connect("database.db", timeout=30, isolation_level=None)
  con.execute('pragma journal_mode=wal')
  
  today_ms = int(time.time() * 1000)
  ms24 = 24 * 60 * 60 * 1000
  yesterday_ms = today_ms - ms24

  for epic in majors:
    parmas = (epic.split(".")[2], yesterday_ms, today_ms)
    df = pd.read_sql_query(
        """
        SELECT datetime, open, high, low, close, volume, p_type, pair, pushed, id
        FROM data
        WHERE pair IS ? AND c_end IS true AND (datetime BETWEEN ? AND ?)
        ORDER BY datetime 
        """,
        con, params=parmas)
    
    df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
    df.set_index('datetime', inplace=True)
    
    sell_df = df[df["p_type"] == "bid"]
    sell_df.ta.adjusted = None
    sell_df.ta.rsi(append=True)
    sell_df.columns
    sell_df['check'] = sell_df.apply(check_signal, axis=1)

    buy_df = df[df["p_type"] == "offer"]
    buy_df.ta.adjusted = None
    buy_df.ta.rsi(append=True)
    buy_df.columns
    buy_df['check'] = buy_df.apply(check_signal, axis=1)
    

#schedule.every().hour.at(":00").do(do_analysis)
schedule.every(10).seconds.do(do_analysis)

while True:
    schedule.run_pending()
    #time.sleep(1800)
    time.sleep(60)
