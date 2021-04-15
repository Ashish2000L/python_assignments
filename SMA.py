'''

    @Environment Specifications
        xlrd : 1.2.0
        numpy : 1.18.5
        pandas : 1.1.4
        matplotlib : 3.3.2
        mysql-connector-python : 8.0.22

'''
import xlrd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector as con


class SMA_Crossover:

    def __init__(self,host:str,user:str,password:str,database:str):
        self.sta = None
        self.lta = None
        self.excel_name = ""

        try:
            #connecting to database
            db = con.connect(host=host,
                             user=user,
                             password=password)
            print("Connection Successful!!\n")
            self.db=db
            cursor=db.cursor()
            # check for database existance
            cursor.execute("show databases")

            dbses = []
            for dbs in cursor:
                dbses.append(dbs[0])

            if database not in dbses:
                cursor.execute("CREATE DATABASE {0}".format(database))
                db.commit()
            dbses.clear()

            cursor.execute("use {0}".format(database))

        except ConnectionError as CON_ERR:
            raise ConnectionError(CON_ERR)

    def createTable(self,table:str):
            # check for table existance
            db = self.db
            cursor = db.cursor()

            cursor.execute("show tables")

            tbls = []
            for tbl in cursor:
                tbls.append(tbl[0])

            if table not in tbls:
                cursor.execute(
                    "CREATE TABLE {0}(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, datetime VARCHAR(10) NOT NULL, close INT NOT NULL,  high INT NOT NULL, low INT NOT NULL, open INT NOT NULL, volume INT NOT NULL, instrument VARCHAR(15) NOT NULL)".format(
                        table))
                self.db.commit()
            tbls.clear()


    def insertDataToTable(self,path_to_excel_file:str,table:str):

            db = self.db
            cursor = db.cursor()

            #checking for the existing data in the table
            cursor.execute("SELECT count(id) from {0}".format(table))
            for i in cursor:
                if i[0]>0:
                    if input("Do you want to truncate existing data ?") =='y':
                        cursor.execute("TRUNCATE TABLE {0}".format(table))
                        self.db.commit()

            try:
                wb = xlrd.open_workbook(path_to_excel_file)
                sheet = wb.sheet_by_index(0)

                values = []
                for i in range(sheet.nrows):
                    if i>1:
                        values.append(tuple(sheet.row_values(i)))

                #inserting data to the table
                cursor.executemany("INSERT INTO {0}(id,datetime,close,high,low,open,volume, instrument) values(NULL,%s,%s,%s,%s,%s,%s,%s)".format(table),values)
                self.db.commit()
            except FileNotFoundError as fnfe:
                raise FileNotFoundError(fnfe)


    def showSMAChart(self,table:str,Short_Term_Average_In_weeks: int, Long_Term_Average_In_Weeks: int):

            if Short_Term_Average_In_weeks< Long_Term_Average_In_Weeks:

                db = self.db
                cursor = db.cursor()

                self.excel_name=table.upper()
                self.sta=Short_Term_Average_In_weeks
                self.lta=Long_Term_Average_In_Weeks

                #checking for the empty table
                cursor.execute("SELECT count(id) from {0}".format(table))
                for i in cursor:
                    if i[0] > 0:
                        cursor.execute("SHOW COLUMNS FROM {0}".format(table))
                        colm_nm = []
                        for delt in cursor:
                            colm_nm.append(delt[0])

                        colm_nm.pop(0)
                        cursor.execute("SELECT * FROM {0} ".format(table))
                        values = []
                        for val in cursor:
                            values.append(val)

                        dict_val = {}
                        for i, column in enumerate(colm_nm):
                            data = [val[i + 1] for val in values]
                            dict_val[column] = data

                        self.__setting_long_short_terms(dict_val)
                    else:
                        raise ValueError("Given Table is Empty, Insert data to the table")
            else:
                raise ValueError("Value of Long_Term_Average_In_Weeks should be greater than Short_Term_Average_In_weeks given {0} & {1} respectively ".format(Long_Term_Average_In_Weeks,Short_Term_Average_In_weeks))


    def __setting_long_short_terms(self, dict_val:dict):
        df = pd.DataFrame(dict_val)

        if self.sta < self.lta:
            sta = str(self.sta) + "_wk_SMA"
            lta = str(self.lta) + "_wk_SMA"

            # Adding column for the average values according to the weeks set by the user
            df[lta] = df["close"].rolling(window=self.lta * 5, min_periods=1).mean()
            df[sta] = df["close"].rolling(window=self.sta * 5, min_periods=1).mean()

            '''Setting Signal values, 
            Signal value here means the point where sta cross over the lta or when lta cross over the sta'''

            df['signal'] = 0.0
            df['signal'] = np.where(df[sta] > df[lta], 1.0, 0.0)

            '''Setting position to check the points where the sta crosses the lta and lta crosses sta.
            1) Position =1 : This means that the sta has cross over the lta and we can buy the share.
            2) Position = -1 : This means that the lta has cross over the sta and we can sell the share '''

            df_pos = pd.DataFrame()
            df['position'] = df['signal'].diff()
            df_pos['position'] = df['signal'].diff()
            df = df.set_index('datetime')
            self.__show_graph(df, df_pos, sta, lta)

        else:
            raise ValueError("Short_Term_Average_In_Weeks should me less them Long_Term_Average_In_Weeks")

    def __show_graph(self, df, df_pos, sta: str, lta: str):

        plt.figure(figsize=(20, 10))
        # plot close price, short-term and long-term moving averages
        df['close'].plot(color='k', label='Close')
        df[sta].plot(color='m', label=sta)
        df[lta].plot(color='g', label=lta)
        # plot ‘buy’ signals
        plt.plot(df_pos[df_pos['position'] == 1].index,
                 df[sta][df['position'] == 1],
                 '^', markersize=15, color='g', label='buy')
        # plot ‘sell’ signals
        plt.plot(df_pos[df_pos['position'] == -1].index,
                 df[sta][df['position'] == -1],
                 'v', markersize=15, color='r', label='sell')
        plt.ylabel('Price in Rupees', fontsize=15)
        plt.xlabel('Date', fontsize=15)
        plt.title(self.excel_name, fontsize=20)
        plt.legend()
        plt.grid()
        plt.show()

