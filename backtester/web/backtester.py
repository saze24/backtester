import itertools
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import sys
import sqlite3 as sq
from sqlite_utils import Database
from datetime import datetime, timedelta
from operator import itemgetter


def log_exceptions(f_path, f_name, exc, desc, line_no):
    # Print and log all exceptions to a file
    
    try:
        exc_info = [f' ~ An exception has occurred. ~', 
                        f'Datetime: \t {datetime.now().replace(microsecond=0)}', 
                        f'File Path: \t {f_path}', 
                        f'File Name: \t {f_name}',
                        f'Exception: \t {exc}', 
                        f'Description: \t {desc}', 
                        f'Line Number: \t {line_no} \n']

        for item in exc_info:
            print(item)
        
        log_path = os.getcwd() + '\\web\\database\\backtester_exceptions.log'
        with open(log_path, "a") as f:
            for line in exc_info:
                f.write(line + '\n')
    
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        print(' ~ An exception has occurred ~')
        print('Datetime: \t', datetime.now().replace(microsecond=0))
        print('File Path: \t',  f_path)
        print('File Name: \t',  f_name)
        print('Exception: \t', exc_type)
        print('Description: \t', exc_obj)
        print('Line Number: \t', exc_tb.tb_lineno)


def data_import_check():
    # Notify web user if there is missing data in csv file or database file is in use
    # Don't proceed until the problems are fixed 

    try:
        data_error = 0

        dir_name = os.getcwd()
        f_name = 'xbtusd_4h_raw.csv'
        raw_path = dir_name + '\\web\\database\\' + f_name

        # Retrieve the market data from csv
        if os.path.exists(raw_path):
            df = pd.read_csv(raw_path)
        else:
            log_exceptions(raw_path, f_name, 'File Not Found', 'Market data csv file not in directory', 87)
            data_error = [f'File not found at {raw_path}',  'Please place the file in the directory to proceed with testing.']
            return 'Data Import Error', data_error

        # Remove ms and slice time range
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Start search 80 hours before start datetime because we need 20 previous periods to calculate MA20
        start_dt = datetime.strptime('2021-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        # 20 time periods * 4 hours = 80 hours
        start_dt = start_dt - timedelta(hours=80)
        start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        df2 = df[(df.timestamp >= start_dt) & (df.timestamp <= '2021-09-01 00:00:00')] 

         # Get the time delta between the datetimes
        df3 = df2.copy()
        df3['timestamp'] = pd.to_datetime(df3['timestamp'], errors='coerce')
        time_delta = df3['timestamp'].diff()[1:]

        # DATETIME
        # List rows where time diff is not 4 hours
        inc_lst = time_delta[time_delta != timedelta(hours=4)] 
        inc_idx = inc_lst.index
     
        # Create a list with datetime of rows where diff is not 4 hours
        inc_datetime = []
        for x in range(len(inc_idx)):
            # Check the original df (not df3) because the inc_idx number starts from the beginning of the file, not the start_dt
            inc_datetime.append(df.iloc[inc_idx[x]-1]['timestamp'])       

        # Data is missing or doesn't conform to the datetime interval of the file.
        if len(inc_datetime) > 0:
            for rec in inc_datetime:
                log_exceptions(dir_name + '\\web\\database\\', 'xbtusd_4h_raw.csv', 'Incomplete Raw Data', \
                    'Data is missing or doesn\'t conform to the datetime interval of the file.', rec)
            # Flash message to web user to ask user to correct the data.  Don't proceed until data is correct.
            dt_err = ', '.join(map(str, inc_datetime)) 
            data_error = [f'Datetime data at {dt_err} is missing or doesn\'t conform at to the datetime interval of the file.', 
            f'Please correct the data at in the {raw_path} file to proceed with testing.']
            return 'Data Import Error', data_error

        # PRICES
        # Change all non-float prices to NaN
        for col in ['open', 'high', 'low', 'close']:
            df3[col] = pd.to_numeric(df3[col], downcast='float', errors='coerce')
        
        # Index of rows with non-float data
        all_nan = df3.loc[pd.isnull(df3).any(1), :].index.values

        # Create a list with datetime of rows where price data is non-numeric
        all_nan_datetime = []
        for x in range(len(all_nan)):
            # Check the original df (not df3) because the inc_idx number starts from the beginning of the file, not the start_dt
            all_nan_datetime.append(df.iloc[all_nan[x]] ['timestamp'])  

         # Data is missing or isn't numeric
        if len(all_nan_datetime) > 0:
            for rec in all_nan_datetime:
                log_exceptions(dir_name + '\\web\\database\\', 'xbtusd_4h_raw.csv', 'Incomplete Raw Data', \
                    'Price data in open, high, low, or close columns is not numeric', rec)
            # Flash message to web user to ask user to correct the data.  Don't proceed until data is correct.
            pr_err = ', '.join(map(str, all_nan_datetime))
            data_error = [ f'Price data in open, high, low, or close columns at {pr_err} is not numeric', 
                f'Please correct the data in the {raw_path} file to proceed with testing.']
            return 'Data Import Error', data_error
    
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)
    
    return 'Data Imported', ''


def db_connect():
    # Connect to the database
   
    try:
        db_path = os.getcwd() + '\\web\\database\\backtester_database.db'
        
        conn = None 
        conn = sq.connect(db_path, timeout=30.0)
        
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return conn


def create_db(test_name, fast_ma_low, fast_ma_high, slow_ma_low, slow_ma_high,
            stop_loss_low, stop_loss_high, take_profit_low, take_profit_high):
    # Create the database and tables.  Transform and load raw market data csv 

    try:
        dir_name = os.getcwd()
        f_name = 'xbtusd_4h_raw.csv'
        raw_path = dir_name + '\\web\\database\\' + f_name
        db_path = dir_name + '\\web\\database\\backtester_database.db'

        # Create a new database
        if not os.path.exists(db_path):
            # Retrieve the market data from csv
            df = pd.read_csv(raw_path)

            conn = None 
            conn = sq.connect(db_path, timeout=30.0)

            # Speed up inserts and reduce DB locks with Write Ahead Logging
            Database(db_path).enable_wal()
            cur = conn.cursor()

            # Drop unneeded columns
            df.drop(['symbol', 'trades', 'volume', 'vwap'], axis=1, inplace=True)

            # Add data for moving averages
            for x in range(3, 21):
                col_name = 'ma' + str(x)
                df[col_name] = round(df['close'].rolling(x).mean(), 2)
            
            # Remove ms and slice time range
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df2 = df[(df.timestamp >= '2021-06-01 00:00:00') & (df.timestamp <= '2021-09-01 00:00:00')] 

            # Get record data from csv file
            instrument_name = f_name.split('_')[0].upper()
            time_frame = f_name.split('_')[1].upper()
            start_time = df2['timestamp'].values[0]
            end_time = df2['timestamp'].values[-1]

            # Create Instrument_Period table
            query = '''CREATE TABLE IF NOT EXISTS Instrument_Period 
                            (Instrument_Period_ID INTEGER, 
                            Instrument_Name TEXT NOT NULL, 
                            Start_Datetime TEXT, 
                            End_Datetime TEXT, 
                            Time_Frame TEXT,
                            CONSTRAINT PK_Instrument_Period_ID PRIMARY KEY (Instrument_Period_ID));'''
            cur.execute(query)

            # Populate Instrument_Period table  
            query = '''INSERT INTO Instrument_Period (Instrument_Name, Start_Datetime, End_Datetime, Time_Frame) 
                        VALUES (?, ?, ?, ?);'''        
            vals = [instrument_name, start_time, end_time, time_frame]                  
            cur.execute(query, vals)
            conn.commit()
            
            instrument_period_dict = {'instrument_period_id': cur.lastrowid, 'instrument_name':instrument_name, \
                                'start_time':start_time, 'end_time':end_time, 'time_frame':time_frame}
            
            # Insert foreign key
            df2.insert(loc=0, column='Instrument_Period_ID', value=cur.lastrowid)

            # Create Market_Data table and populate it
            query = '''CREATE TABLE  IF NOT EXISTS Market_Data 
                        (Market_Data_ID INTEGER,
                        Instrument_Period_ID INTEGER,
                        Timestamp TEXT, Open REAL, High REAL, Low REAL, Close REAL, MA3 REAL, MA4 REAL, MA5 REAL, 
                        MA6 REAL, MA7 REAL, MA8 REAL, MA9 REAL, MA10 REAL, MA11 REAL, MA12 REAL, MA13 REAL, 
                        MA14 REAL, MA15 REAL, MA16 REAL, MA17 REAL, MA18 REAL, MA19 REAL, MA20 REAL,
                        CONSTRAINT PK_Market_Data_ID PRIMARY KEY (Market_Data_ID), 
                        FOREIGN KEY(Instrument_Period_ID) REFERENCES Instrument_Period(Instrument_Period_ID));'''       
            cur.execute(query)
            df2.to_sql('Market_Data', conn, if_exists='append', index=False)

            # Create Test_Variable_Range table
            query = '''CREATE TABLE  IF NOT EXISTS Test_Variable_Range
                        (Test_Variable_Range_ID INTEGER, 
                        Instrument_Period_ID INTEGER,
                        Test_Name TEXT NOT NULL UNIQUE,
                        Fast_MA_Low INTEGER, 
                        Fast_MA_High INTEGER, 
                        Slow_MA_Low INTEGER, 
                        Slow_MA_High INTEGER, 
                        Stop_Loss_Low REAL,
                        Stop_Loss_High REAL,
                        Take_Profit_Low REAL,
                        Take_Profit_High REAL,
                        CONSTRAINT Test_Variable_Range_ID PRIMARY KEY (Test_Variable_Range_ID), 
                        FOREIGN KEY(Instrument_Period_ID) REFERENCES Instrument_Period(Instrument_Period_ID));'''
            cur.execute(query)          

            # Populate Test_Variable_Range table    
            query = '''INSERT INTO Test_Variable_Range (Instrument_Period_ID, Test_Name, Fast_MA_Low, Fast_MA_High, 
                    Slow_MA_Low, Slow_MA_High, Stop_Loss_Low, Stop_Loss_High, Take_Profit_Low, Take_Profit_High) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''' 
            vals = [instrument_period_dict['instrument_period_id'], test_name, fast_ma_low, fast_ma_high, slow_ma_low, \
                    slow_ma_high, stop_loss_low/100, stop_loss_high/100, take_profit_low/100, take_profit_high/100]
            cur.execute(query, vals)
            conn.commit()
            test_variable_range_id = cur.lastrowid

            # Create Strategy_Results table
            query = '''CREATE TABLE  IF NOT EXISTS Strategy_Results
                        (Strategy_Results_ID INTEGER, 
                        Test_Variable_Range_ID INTEGER,
                        Fast_MA INTEGER,
                        Slow_MA INTEGER,
                        Stop_Loss REAL, 
                        Take_Profit REAL, 
                        Total_PNL REAL,
                        CONSTRAINT Strategy_Results_ID PRIMARY KEY (Strategy_Results_ID), 
                        FOREIGN KEY(Test_Variable_Range_ID) REFERENCES Test_Variable_Range(Test_Variable_Range_ID));'''
            cur.execute(query)

            # Create Position_Details Table
            query = '''CREATE TABLE  IF NOT EXISTS Position_Details
                        (Position_Details_ID INTEGER,
                        Strategy_Results_ID INTEGER, 
                        Direction TEXT,
                        Open_Time TEXT,
                        Open_Price REAL,
                        Close_Time TEXT,
                        Close_Price REAL,
                        PNL REAL,
                        CONSTRAINT Position_Details_ID PRIMARY KEY (Position_Details_ID), 
                        FOREIGN KEY(Strategy_Results_ID) REFERENCES Strategy_Results(Strategy_Results_ID));'''
            cur.execute(query)

            query = '''SELECT * FROM Instrument_Period WHERE Instrument_Name = "XBTUSD"'''
            cur.execute(query)
            res = list(cur.fetchone())

            instrument_period_dict = []
            col = [desc[0].lower() for desc in cur.description]
            instrument_period_dict = dict(zip(col, res))

            if conn:
                conn.close()

        # Update existing database
        else:
            conn = None 
            conn = sq.connect(db_path, timeout=30.0)

            # Speed up inserts and reduce DB locks with Write Ahead Logging
            Database(db_path).enable_wal()
            cur = conn.cursor()

            query = '''SELECT * FROM Instrument_Period WHERE Instrument_Name = "XBTUSD"'''
            cur.execute(query)
            res = list(cur.fetchone())

            instrument_period_dict = []
            col = [desc[0].lower() for desc in cur.description]
            instrument_period_dict = dict(zip(col, res))

            # Populate Test_Variable_Range table
            query = '''INSERT INTO Test_Variable_Range (Instrument_Period_ID, Test_Name, Fast_MA_Low, Fast_MA_High, 
                    Slow_MA_Low, Slow_MA_High, Stop_Loss_Low, Stop_Loss_High, Take_Profit_Low, Take_Profit_High) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''' 
            vals = [instrument_period_dict['instrument_period_id'], test_name, fast_ma_low, fast_ma_high, slow_ma_low, \
                    slow_ma_high, stop_loss_low/100, stop_loss_high/100, take_profit_low/100, take_profit_high/100]
            cur.execute(query, vals)
            conn.commit()
            test_variable_range_id = cur.lastrowid
           
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return instrument_period_dict, test_variable_range_id

class Test_Strategy:
    """ Populate the database with the results from the test """


    def __init__(self, rec_dict, fast_ma, slow_ma, stop_loss, take_profit, instrument_period_dict, test_variable_range_id):
        
        self.rec_dict = rec_dict
        self.fast_ma = 'ma' + str(fast_ma)
        self.slow_ma = 'ma' + str(slow_ma)
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.instrument_period_dict = instrument_period_dict
        self.test_variable_range_id = test_variable_range_id
        self.short_position = []
        self.long_position = []
        self.open_position()       


    def open_position(self, start_idx=0):
        # Take a position, either long (buy) or short (sell), when the fast_ma crosses the slow_ma
        # Next run close_position
    
        try:
            self.start_idx = start_idx          
            
            # Search for a crossover
            for self.start_idx in range(self.start_idx, len(self.rec_dict) - 1):
                
                # Short when the fast_ma crosses under slow_ma
                # Check the price at start_idx was OVER the slow_ma and then the price at start_idx+1 FELL UNDER the slow_ma
                if self.rec_dict[self.start_idx][self.fast_ma] > self.rec_dict[self.start_idx][self.slow_ma] and \
                    self.rec_dict[self.start_idx+1][self.fast_ma] < self.rec_dict[self.start_idx+1][self.slow_ma]:
                    # Short the open of the next candle start_idx+2
                    self.short_position.append( {'direction':'short', 'open_time':self.rec_dict[self.start_idx+2]['timestamp'], 
                        'open_price':self.rec_dict[self.start_idx+2]['open']} )
                    # Start search to close the position as soon as it is opened, start_idx+2
                    self.start_idx += 2
                    self.close_position(direction = 'short', start_idx=self.start_idx)
                    return
                
                # Long when the fast_ma crosses over ms_slow
                # Check the price at start_idx was UNDER the slow_ma and then the price at start_idx+1 ROSE OVER the slow_ma
                elif self.rec_dict[self.start_idx][self.fast_ma] < self.rec_dict[self.start_idx][self.slow_ma] and \
                    self.rec_dict[self.start_idx+1][self.fast_ma] > self.rec_dict[self.start_idx+1][self.slow_ma]:
                    # Long the open of the next candle start_idx+2
                    self.long_position.append( {'direction':'long', 'open_time':self.rec_dict[self.start_idx+2]['timestamp'], 
                        'open_price':self.rec_dict[self.start_idx+2]['open']} )
                    # Start search to close the position as soon as it is opened, start_idx+2
                    self.start_idx += 2
                    self.close_position(direction = 'long', start_idx=self.start_idx)
                    return
        
            # End search at the end of the market data
            else:
                self.load_results()
                return
    
        except BaseException:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
            log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)


    def close_position(self, direction, start_idx):
        # After a position, either long (buy) or short (sell), has been taken, 
        # close the position as soon as price reaches either the stop_loss or the take_profit
        # If are reached in the time frame, close the position at the stop_loss price
        # Next look for a new postion with open_position
        
        try:
            self.direction = direction
            self.start_idx = start_idx

            if self.direction == 'short':

                # Exchange price increments are 0.5, so round off the exit prices
                self.sl_price = round(self.short_position[-1]['open_price'] * (1.0 + self.stop_loss) * 2) / 2
                self.tp_price = round(self.short_position[-1]['open_price'] * (1.0 - self.take_profit) * 2) / 2

                # Close the short
                for self.start_idx in range(self.start_idx, len(self.rec_dict)-1):
                    # Loss.  Price went up and hit your stop_loss
                    if self.rec_dict[self.start_idx]['high'] >= self.sl_price:
                        self.short_position[-1].update( {'close_time':self.rec_dict[self.start_idx]['timestamp'], 
                            'close_price':self.sl_price, 'pnl':-self.stop_loss} )                      
                        # Look for a new position on the next candle
                        self.start_idx += 1
                        self.open_position(start_idx=self.start_idx)
                        return

                    # Profit.  Price went down and hit your take_profit
                    elif self.rec_dict[self.start_idx]['low'] <= self.tp_price:
                        self.short_position[-1].update( {'close_time':self.rec_dict[self.start_idx]['timestamp'], 
                            'close_price':self.tp_price, 'pnl':self.take_profit} )
                        # Look for a new position on the next candle
                        self.start_idx += 1
                        self.open_position(start_idx=self.start_idx)
                        return

                # Close the position at end of the market data at the last close price
                else:
                    self.urpnl = round( ( self.short_position[-1]['open_price'] - self.rec_dict[-1]['close'] ) \
                        / self.short_position[-1]['open_price'], 4 )
                    self.short_position[-1].update( {'close_time':self.rec_dict[-1]['timestamp'], \
                        'close_price':self.rec_dict[-1]['close'], 'pnl':self.urpnl} )
                    self.load_results()
                    return

            elif self.direction == 'long':
                # Exchange price increments are 0.5, so round off the exit prices
                self.sl_price = round(self.long_position[-1]['open_price'] * (1.0 - self.stop_loss) * 2) / 2
                self.tp_price = round(self.long_position[-1]['open_price'] * (1.0 + self.take_profit) * 2) / 2

                # Close the long
                for self.start_idx in range(self.start_idx, len(self.rec_dict)-1):
                    # Loss.  Price went down and hit your stop_loss
                    if self.rec_dict[self.start_idx]['low'] <= self.sl_price:
                        self.long_position[-1].update( {'close_time':self.rec_dict[self.start_idx]['timestamp'], 
                            'close_price':self.sl_price, 'pnl':-self.stop_loss} )
                        # Look for a new position on the next candle
                        self.start_idx += 1
                        self.open_position(start_idx=self.start_idx)
                        return

                    # Price went up and hit your take_profit
                    elif self.rec_dict[self.start_idx]['high'] >= self.tp_price:
                        self.long_position[-1].update( {'close_time':self.rec_dict[self.start_idx]['timestamp'], 
                            'close_price':self.tp_price, 'pnl':self.take_profit} )
                        # Look for a new position on the next candle
                        self.start_idx += 1
                        self.open_position(start_idx=self.start_idx)
                        return

                # Close the position at end of the market data at the last close price 
                else:                 
                    self.urpnl = round( ( self.rec_dict[-1]['close'] - self.long_position[-1]['open_price'] ) \
                        / self.long_position[-1]['open_price'], 4 )
                    self.long_position[-1].update( {'close_time':self.rec_dict[-1]['timestamp'], \
                        'close_price':self.rec_dict[-1]['close'], 'pnl':self.urpnl} )
                    self.load_results()
                    return

        except BaseException:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
            log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)


    def load_results(self):
        # Insert every order in the Position_Details table (many records)
        # Add up the PNL and insert the final results of test in the Strategy_Results (1 record)

        try:
            self.pnl_results = []           
            self.short_pnl = 0
            self.total_wins = 0
            # Sum results for total_pnl, number of trades, number of winning trades
            for x in range(len(self.short_position)):
                for k, v in self.short_position[x].items():
                    if k == 'pnl':
                        self.short_pnl += v
                        if v > 0.0:
                            self.total_wins += 1

            self.long_pnl = 0
            # Sum results for total_pnl, number of trades, number of winning trades
            for x in range(len(self.long_position)):
                for k, v in self.long_position[x].items():
                    if k == 'pnl':
                        self.long_pnl += v
                        if v > 0.0:
                            self.total_wins += 1
            
            self.total_pnl = round(self.short_pnl + self.long_pnl, 2)

            self.pnl_results.append( {'start_time':self.rec_dict[0]['timestamp'], 'end_time':self.rec_dict[-1]['timestamp'], \
                                'stop_loss':self.stop_loss, 'take_profit':self.take_profit, 'total_pnl':self.total_pnl } )
            
            self.conn = db_connect()
            self.cur = self.conn.cursor()

            # Populate Strategy_Results table
            # Change to dataframe to sql for speed    
            self.query = '''INSERT INTO Strategy_Results (Test_Variable_Range_ID, Fast_MA, Slow_MA, Stop_Loss, Take_Profit, Total_PNL) 
                                VALUES (?, ?, ?, ?, ?, ?);'''

            # Remove 'ma' and just enter the ma number in the table 
            self.vals = [self.test_variable_range_id, self.fast_ma.split('ma')[1], self.slow_ma.split('ma')[1], 
                            self.stop_loss, self.take_profit, self.total_pnl]
           
            self.cur.execute(self.query, self.vals)
            self.conn.commit()
            self.strategy_results_id = self.cur.lastrowid

            # Create a timestamp ordered nested list of all the positions taken
            #{'direction': 'short', 'open_time': '2021-06-05 04:00:00', 'open_price': 37432.5, 'close_time': '2021-06-05 08:00:00', 'close_price': 35935.0, 'pnl': 0.04}
            self.pos = []
            self.position = []
            self.pos = self.short_position + self.long_position
            self.position = sorted(self.pos, key=itemgetter('open_time'))
            
            #'''
            # Populate Position_Detail table
            self.position_details_df = pd.DataFrame.from_dict(self.position)
            # Insert foreign key
            self.position_details_df.insert(loc=0, column='Strategy_Results_ID', value=self.strategy_results_id)
            self.position_details_df.to_sql('Position_Details', self.conn, if_exists='append', index=False)
            self.conn.commit()
            #'''
            
            if self.conn:
                self.conn.close()

        except BaseException:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
            log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)


def plot_chart(strategy_results_id):
    # Display an HTML chart that shows the market, moving averages, trades, and PNL data in a web browser

    try:
        conn = db_connect()
        cur = conn.cursor()

        # Retrienve the results of a selected strategy
        cur.execute ('''SELECT Fast_MA, Slow_MA FROM Strategy_Results 
                WHERE Strategy_Results_ID=\'{s_r_id}\''''.format(s_r_id=strategy_results_id))
        ma_length = list(cur.fetchone())

        # Retrieve every order for a selected strategy
        cur.execute ('''SELECT * FROM Position_details 
                WHERE Strategy_Results_ID=\'{s_r_id}\''''.format(s_r_id=strategy_results_id))
        position_details = list(cur.fetchall())

        df = pd.read_sql_query("SELECT * FROM Market_Data", conn)
        
        # Create dataframes for the MAs
        f_ma = 'MA'+ str(ma_length[0])
        s_ma = 'MA'+ str(ma_length[1])
        df_fast_ma = df[['Timestamp', f_ma]]
        df_slow_ma = df[['Timestamp', s_ma]]
        
        if conn:
            conn.close()

        #{'direction': 'short', 'open_time': '2021-08-30 12:00:00', 'open_price': 47909.5, 'close_time': '2021-09-01 00:00:00', 'close_price': 47046.5, 'pnl': 0.018}
        # Create a figure the the price and MA data
        fig = go.Figure( data = [ go.Candlestick (
            name='XBTUSD',
            x=df['Timestamp'],
            open=df['Open'], high=df['High'],
            low=df['Low'], close=df['Close'],
            increasing_line_color='green', decreasing_line_color='red' ),
            go.Scatter( x=df['Timestamp'], y=df_fast_ma[f_ma], line=dict(color='purple', width=2), name=f_ma.upper() ),
            go.Scatter( x=df['Timestamp'], y=df_slow_ma[s_ma], line=dict(color='blue', width=2), name=s_ma.upper() ), 
            ] )

        # Disable the range slider at the bottom of the chart as it prevents the auto-resizing of the y-axis
        #go.Layout(xaxis = dict(rangeslider = dict(visible = False)))

        # Make lists of all the long and short positons taken for a given strategy
        shorts_o, shorts_c = [], []
        longs_o, longs_c = [], []
        for x in range(len(position_details)):
            if position_details[x][2] == 'short':
                shorts_o.append( {'open_time':position_details[x][3], 'open_price':position_details[x][4]} )
                longs_c.append( {'close_time':position_details[x][5], 'close_price':position_details[x][6], 'pnl':position_details[x][7]} )
            elif position_details[x][2] == 'long':
                longs_o.append( {'open_time':position_details[x][3], 'open_price':position_details[x][4]} )
                shorts_c.append( {'close_time':position_details[x][5], 'close_price':position_details[x][6], 'pnl':position_details[x][7]} )
       
        #{'direction': 'short', 'open_time': '2021-05-04 08:00:00', 'open_price': 55953.0, 'close_time': '2021-05-04 12:00:00', 'close_price': 53715.0, 'pnl': 0.04}
        # Display all the sell orders that opened short postions
        for item in shorts_o:
            fig.add_annotation(x=item['open_time'], 
                y=item['open_price'], ay=-60,
                text='<b>Open Short</b>',
                font=dict(color='rgb(231,41,138)', size=15),
                hovertext='Sell Price: ' + str(item['open_price']), 
                hoverlabel=dict(bgcolor='rgb(231,41,138)'),
                showarrow=True, ax=-10, standoff=2, 
                arrowcolor='black', arrowwidth=1.2,
                arrowhead=1, arrowsize=1.2)

        # Display the sell orders that closed long positions
        for item in longs_c:
            fig.add_annotation(x=item['close_time'], 
                y=item['close_price'], ay=-60,
                text='<b>Close Short</b>',
                font=dict(color="rgb(231,41,138)", size=15),
                hovertext='Sell Price: ' + str(item['close_price']) + ', PNL: ' + str(round(item['pnl']*100,1)) + '%', 
                hoverlabel=dict(bgcolor='rgb(231,41,138)'),
                showarrow=True, ax=-10, standoff=2,
                arrowcolor='black', arrowwidth=1.2,
                arrowhead=1, arrowsize=1.2)
        
        # Display all the buy orders that opened long postions
        for item in longs_o:
            fig.add_annotation(x=item['open_time'], 
                y=item['open_price'], ay=60, 
                text='<b>Open Long</b>',
                font=dict(color='#316395', size=15),
                hovertext='Buy Price: ' + str(item['open_price']), 
                hoverlabel=dict(bgcolor='#316395'), yanchor="bottom",
                showarrow=True, ax=-10, standoff=2,
                arrowcolor='black', arrowwidth=1.2,
                arrowhead=1, arrowsize=1.2)

        # Display the buy orders that closed short positions
        for item in shorts_c:
            fig.add_annotation(x=item['close_time'], 
                y=item['close_price'], ay=60,
                text='<b>Close Long</b>',
                font=dict(color='#316395', size=15),
                hovertext='Buy Price: ' + str(item['close_price']) + ', PNL: ' + str(round(item['pnl']*100,1)) + '%', 
                hoverlabel=dict(bgcolor='#316395'), yanchor="bottom",
                showarrow=True, ax=-10, standoff=2,
                arrowcolor='black', arrowwidth=1.2,
                arrowhead=1, arrowsize=1.2)

        #Disable the range slider at the bottom, so y-axis autoscales on zoom      
        fig.update_layout(xaxis_rangeslider_visible=False)

        #fig.show()
        pio.write_html(fig, file='web/templates/ohcl_chart.html', auto_open=False, full_html=False)

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)


def retrieve_top_strats(test_variable_range_id):
    # Retrieve the data for the top 50 strategies to show as a test summary

    try:
        conn = db_connect()
        cur = conn.cursor()
        query = f'''SELECT * FROM Strategy_Results 
                    WHERE Test_Variable_Range_ID = {test_variable_range_id}
                    ORDER BY Total_PNL DESC LIMIT 50'''
        cur.execute(query)
        # Map column names to field values in nested dictionary
        res = list(cur.fetchall())       
        col = [desc[0].lower() for desc in cur.description]
        top_strats = []

        # Change float from 0.05 to 5%
        res = [list(t) for t in res]

        for x in range(len(res)):
            res[x][4] = str( round(res[x][4]*100, 1) ) + '%'
            res[x][5] = str( round(res[x][5]*100, 1) ) + '%'
            res[x][6] = str( round(res[x][6]*100, 1) ) + '%'

        # Zip the columns and fields into a nested dictionary    
        for row in res:
            dic = dict(zip(col, row))
            top_strats.append(dic)  
                
        if conn:
            conn.close()

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return top_strats


def retrieve_top_group_strats(test_variable_range_id):
    # Of the top 200 individual strategies, group those with the same fast_ma and slow_ma
    # Display the group's averages and how many individual results it has in the top 200

    try:
        conn = db_connect()
        cur = conn.cursor()

        query = f'''WITH cte_ma (f_ma, s_ma, sl, tp, tot_pnl) AS (
                    SELECT Fast_MA, Slow_MA, Stop_Loss, Take_Profit, Total_PNL
                    FROM Strategy_Results
                    WHERE Test_Variable_Range_ID = {test_variable_range_id}
                    ORDER BY Total_PNL DESC
                    LIMIT 200),

                    cte_pnl (f_ma_p, s_ma_p, top_pnl) AS (
                    SELECT Fast_MA, Slow_MA, MAX(Total_PNL)
                    FROM Strategy_Results
                    WHERE Test_Variable_Range_ID = {test_variable_range_id}
                    GROUP BY Fast_MA, Slow_MA)

                    SELECT m.f_ma as Fast_MA, m.s_ma as Slow_MA, 
                            AVG(sl) Avg_SL, AVG(tp) as AVG_TP, p.top_pnl as Top_PNL,
                            AVG(tot_pnl) as Avg_PNL, count(*) as Freq
                    FROM cte_ma AS m
                    JOIN cte_pnl AS p
                    ON (m.f_ma = p.f_ma_p and m.s_ma = p.s_ma_p)
                    GROUP BY f_ma, s_ma
                    HAVING Freq > 2
                    ORDER BY avg_pnl DESC;''' 
        
        cur.execute (query)
        res = list(cur.fetchall())
        col = [desc[0].lower() for desc in cur.description]
        top_group_strats = []

        # Change float from 0.05 to 5%, add row number at the end
        res = [list(t) for t in res]
        for x in range(len(res)):
            res[x][2] = str( round(res[x][2]*100, 1)) + '%'
            res[x][3] = str( round(res[x][3]*100, 1)) + '%'
            res[x][4] = str( round(res[x][4]*100, 1)) + '%'
            res[x][5] = str( round(res[x][5]*100, 1)) + '%'
            res[x].append(x)
        
        #Map column names to field values in nested dictionary    
        for row in res:
            dic = dict(zip(col, row))
            top_group_strats.append(dic)
        
        if conn:
            conn.close()
       
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return top_group_strats


def cartesian_product(fast_ma_low, fast_ma_high, slow_ma_low, slow_ma_high, stop_loss_low, stop_loss_high,
             take_profit_low, take_profit_high, instrument_period_dict, test_variable_range_id):
    # Combine the range of variables so that every possible permutation can be tested
    
    try:
        # List variable ranges
        fast_ma = [x for x in range(fast_ma_low, fast_ma_high+1)]
        slow_ma = [x for x in range(slow_ma_low, slow_ma_high+1)]
        stop_loss = [x/100 for x in range(stop_loss_low, stop_loss_high+1)]
        take_profit = [x/100 for x in range(take_profit_low, take_profit_high+1)]

        # Generate cartesian product i.e. a many-to-many join
        cart_prod = itertools.product(fast_ma, slow_ma, stop_loss, take_profit)

        cart_prod_list = []
        for x in cart_prod:   
            cart_prod_list.append(x)
        
        # Rules:  Only include items where take_profit is higher than stop_loss and fast_ma period is less than slow_ma
        variable_list = []
        for x in range(len(cart_prod_list)):
            if cart_prod_list[x][3] >= cart_prod_list[x][2] + 0.01 and cart_prod_list[x][0] < cart_prod_list[x][1]:
                variable_list.append((cart_prod_list[x][0], cart_prod_list[x][1], cart_prod_list[x][2], \
                    cart_prod_list[x][3], instrument_period_dict, test_variable_range_id))
        

        no_of_tests = len(variable_list)
    
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return variable_list, no_of_tests


def run_test(cart_list):
    # Multiprocessing is necessary to complete the tests in a timely fashion
    # This function will be called by the run_tests function in \web\views.py using a pool of workers
    
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute ('SELECT * FROM Market_Data')
        
        # Map column names to field values in nested dictionary
        rec_list = list(cur.fetchall())       
        col = [desc[0].lower() for desc in cur.description]
        rec_dict = []

        # Create a  market data dictionary
        for row in rec_list:
            dic = dict(zip(col, row))
            rec_dict.append(dic)
        
        if conn:
            conn.close()

        # Create an instance to start the test
        Test_Strategy(rec_dict, cart_list[0], cart_list[1], cart_list[2], cart_list[3], cart_list[4], cart_list[5])

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)



# Only run when imported
if __name__ == "__main__":
    print(' ~ An exception has occurred ~')
    print('File Name: \t backtester.py')
    print('Exception:  \t This file is designed to be impported by \\web\\views.py')
