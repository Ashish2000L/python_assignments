import SMA

path="path_to_the_excel_file"
database = 'database_name'
table = 'table_name'.lower()

#connecting to MySql and creating database if not exist
sma=SMA.SMA_Crossover('localhost','root','',database)

#for creating table if not exist
sma.createTable(table)

#inserting data to the table
sma.insertDataToTable(path,table)

#show chart for the 'simple moving average crossover'
sma.showSMAChart(table,2,8)