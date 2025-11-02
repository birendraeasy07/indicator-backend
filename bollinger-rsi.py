from fyers_apiv3 import fyersModel
from datetime import datetime, timedelta
import pandas as pd
import ta

COMPLETED = True
if COMPLETED:
    NIFTY50=["ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK","BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BEL",\
            "BHARTIARTL","CIPLA","COALINDIA","DRREDDY","EICHERMOT","GRASIM","HCLTECH","HDFCBANK","HDFCLIFE","HEROMOTOCO",\
            "HINDALCO","HINDUNILVR","ICICIBANK","ITC","INDUSINDBK","INFY","JSWSTEEL","JIOFIN","KOTAKBANK","LT","M&M",\
            "MARUTI","NTPC","NESTLEIND","ONGC","POWERGRID","RELIANCE","SBILIFE","SHRIRAMFIN","SBIN","SUNPHARMA","TCS",\
            "TATACONSUM","TATAMOTORS","TATASTEEL","TECHM","TITAN","TRENT","ULTRACEMCO","WIPRO","ZOMATO"]
else:
    NIFTY50=["HINDALCO","HINDALCO"]
class BollingerRSI:
    fyers = None
    def __init__(self):
        self.client_id = "DLR9YW5108-100"
        self.secret_key = "RBXZDAR695"
        self.redirect_uri = "https://www.google.com/"
        self.response_type = "code"  
        self.state = "sample_state"
        self.grant_type = "authorization_code"  
        self.session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type=self.response_type
                )
        self.auth_url = self.session.generate_authcode()
        self.auth_code = None

    def get_auth_url(self):
        print("url:",self.auth_url)
        value_url = input("Enter auth value: ")        
        return value_url

    def set_session(self):
        self.session = fyersModel.SessionModel(
        client_id=self.client_id,
        secret_key=self.secret_key, 
        redirect_uri=self.redirect_uri, 
        response_type=self.response_type, 
        grant_type=self.grant_type
        )         
        self.session.set_token(self.auth_code)
        result = self.session.generate_token()
        return result
    
    def setup(self,auth_code):
        self.auth_code =  auth_code.split("auth_code=")[-1].split("&state")[0]
        print("auth_code",self.auth_code)
        response = self.set_session()
        access_token = response['access_token']
        self.fyers = fyersModel.FyersModel(client_id=self.client_id, is_async=False, token=access_token, log_path="")
        response = self.fyers.get_profile()
        if response['s'] == 'ok':
            print("successful setup fyers")
        else:
            print("Failed to setup fyers")


    def generate_buy_sell_signals(self,df,profit_per):
        # Initialize columns
        is_stock_hold = False
        bought_price = 0
        df['buy_at'] = 0
        df['sell_at'] = 0
        df['holding'] = False  
        df['entry_price'] = 0  
        for i in range(1, len(df)):
            current_price = df['Close'].iloc[i]
            if not df['holding'].iloc[i-1]:  # If we're not holding a position
                if (current_price <= df['bb_lower'].iloc[i-1]) \
                    and df['rsi'].iloc[i-1] < 30 \
                    and current_price <= df['bb_lower'].iloc[i] \
                    and df['rsi'].iloc[i] < 30:
                    df.loc[df.index[i], 'buy_at'] = current_price
                    df.loc[df.index[i], 'holding'] = True
                    df.loc[df.index[i], 'entry_price'] = current_price
                    is_stock_hold = True
                    bought_price= current_price
                
            else:  # If we're holding a position
                if is_stock_hold:
                    percentage_profit = (profit_per / 100 ) + 1.0
                    target_price = bought_price * percentage_profit  # 2.5% profit target
                    df.loc[df.index[i], 'holding'] = True
                    df.loc[df.index[i], 'entry_price'] = bought_price
                
                # Check if we hit our profit target
                if df.loc[df.index[i],'Close'] >= target_price:
                    df.loc[df.index[i], 'sell_at'] = target_price
                    df.loc[df.index[i], 'holding'] = False
                    df.loc[df.index[i], 'entry_price'] = 0
        
        return df

    def calculate_profits(self,df):
        trades = df[['Timestamp','script','buy_at', 'sell_at']].copy()
        trades = trades[(trades['buy_at'] != 0) | (trades['sell_at'] != 0)]
        trades['profit'] = 0.0
        trades['holding_days'] = 0
        trades['Timestamp'] = pd.to_datetime(trades['Timestamp'], format='%d-%m-%Y %H:%M:%S')
        buy_data = trades[trades['buy_at'] != 0][['Timestamp', 'buy_at']]
        sell_data = trades[trades['sell_at'] != 0][['Timestamp', 'sell_at']]
        for i in range(min(len(buy_data), len(sell_data))):
            buy_price = buy_data.iloc[i]['buy_at']
            sell_price = sell_data.iloc[i]['sell_at']
            buy_date = buy_data.iloc[i]['Timestamp']
            sell_date = sell_data.iloc[i]['Timestamp']
            
            profit = sell_price - buy_price
            days_held = (sell_date - buy_date).days
            
            sell_idx = trades[trades['sell_at'] == sell_price].index[0]
            trades.loc[sell_idx, 'profit'] = profit if profit > 0 else 0
            trades.loc[sell_idx, 'holding_days'] = days_held
        
        trades['Timestamp'] = trades['Timestamp'].dt.strftime('%d-%m-%Y %H:%M:%S')
        return trades

    def stock_script_created(self,script,interval,start_date,time_dalta,end_date):
        try:
            symbol = script.upper()
            from_date = start_date 
            if time_dalta:
                to_date = (datetime.now() - timedelta(days=200)).strftime('%Y-%m-%d')
            else:
                to_date = datetime.now().strftime('%Y-%m-%d')
            data = {
            "symbol":f"NSE:{symbol}-EQ",
            "resolution":interval,
            "date_format":"1",
            "range_from":from_date,
            "range_to":to_date,
            "cont_flag":"1"
            }
            print('script details is created',data)
            return data
        except Exception as e:
            print(f"Error in initializing fyers {e}")
        
    def execute(self,profit_per=2.5, interval = 'D',start_date="2025-01-01",time_dalta=None,end_date=None):
        df_result = pd.DataFrame()
        for script in NIFTY50:
            df = None
            data = self.stock_script_created(script,interval,start_date,time_dalta,end_date)
            response = self.fyers.history(data=data)
            if 'candles' in response and response['candles']:
                df = pd.DataFrame(response['candles'], columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s').dt.strftime('%d-%m-%Y %H:%M:%S')
                df['script'] = script
                indicator_bb = ta.volatility.BollingerBands(close=df["Close"], window=20, window_dev=2)
                df['bb_upper'] = indicator_bb.bollinger_hband()
                df['bb_lower'] = indicator_bb.bollinger_lband()
                # Add Bollinger Band indicators
                df['bb_buy_signal'] = indicator_bb.bollinger_lband_indicator()  # Buy when price crosses below lower band
                df['bb_sell_signal'] = indicator_bb.bollinger_hband_indicator()  # Sell when price crosses above upper band
                df['bb_percent'] = indicator_bb.bollinger_pband()  # Relative position between upper and lower bands
                # Calculate RSI
                indicator_rsi = ta.momentum.RSIIndicator(close=df['Close'], window=14)  # 14 is the standard period for RSI
                df['rsi'] = indicator_rsi.rsi()  # Add RSI values to dataframe                
            else:
                print(f"DataFrame is not fetched")
            # print("df::",df)
            df = self.generate_buy_sell_signals(df,profit_per)
            
            df_profit = self.calculate_profits(df)
            df_result = pd.concat([df_result,df_profit],axis=0)
            df_result.to_csv("BollingerRSI.csv")

    
bollinger_rsi = BollingerRSI()
auth_url = bollinger_rsi.get_auth_url()
bollinger_rsi.setup(auth_url)
bollinger_rsi.execute(profit_per=4,interval = 'D',start_date="2025-02-03",time_dalta=None,end_date=None)
