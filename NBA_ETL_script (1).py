import pandas as pd
import numpy as np 
import datetime 
import pymysql

#Create a connection to MySQL database

connection = pymysql.connect(host='127.0.0.1',
                        user='root',
                        password='INPUT PASSWORD',
                        db='CREATE DATABASE',
                    cursorclass=pymysql.cursors.DictCursor)

print(connection)

cursor = connection.cursor()

#Load in the downloaded Excel files from Basketball-reference.com 

Curry = pd.read_excel("Stephen_Curry.xlsx")
Lillard = pd.read_excel("Damian_Lillard.xlsx")
Mitchell = pd.read_excel("Donovan_Mitchell.xlsx")
Paul = pd.read_excel("Chris_Paul.xlsx")
Fox = pd.read_excel("DeAaron_Fox.xlsx")

#Put the dataframes into a list for some quick preliminary cleaning/transforming

data = [Curry, Lillard, Mitchell, Paul, Fox]

for i in data:
    i.drop(['Rk', 'G', 'Unnamed: 5','MP', 'GmSc'], axis=1, inplace=True)
    i=i.rename(columns={"Unnamed: 7": "Win_Loss","+/-":"Plus_Minus","Tm":"Team_ID",'GS':'GP','Date':'Date_ID','3P%':'3P_Percent', 'FG%':'FG_Percent','FT%':'FT_Percent'}, inplace=True)
    
for i in data:
    i['Postseason'] = i['Postseason'].astype(str)
    i['GP'] = i['GP'].astype(str)
    i['GP'] = i['GP'].replace('1','Played')
    i['GP'] = i['GP'].replace('0','Played')
    i['Postseason'] = i['Postseason'].replace('nan','NP')

for i in data:
    i['Win_Loss'] = i['Win_Loss'].astype(str)
    i[['Win_Loss','Differential']] = i['Win_Loss'].str.split('(',expand=True)
    i['Win_Loss'] = i['Win_Loss'].map(lambda x: x.strip('('))
    i['Differential'] = i['Differential'].map(lambda x: x.strip(')'))
    i['Differential'] = i['Differential'].map(lambda x: x.strip('+'))
    a = 'Differential'
    temp_col = i.pop(a)
    i.insert(5, a, temp_col)
    i['Differential'] = i['Differential'].astype(int)

Curry = Curry.fillna(0,axis = 0)
Lillard = Lillard.fillna(0,axis = 0)
Mitchell = Mitchell.fillna(0,axis = 0)
Paul = Paul.fillna(0,axis = 0)
Fox = Fox.fillna(0,axis = 0)

#Insert in the IDs eg.(PK and FK) for the dimensions and fact table

Curry.insert(0, 'Player_ID', 'SC_30')
Lillard.insert(0, 'Player_ID', 'DL_0')
Mitchell.insert(0, 'Player_ID', 'DM_45')
Paul.insert(0, 'Player_ID', 'CP_3')
Fox.insert(0, 'Player_ID', 'DF_5')

Curry.insert(1, 'Coach_ID', 'S_Kerr')
Lillard.insert(1, 'Coach_ID', 'T_Stotts')
Mitchell.insert(1, 'Coach_ID', 'Q_Snyder')
Paul.insert(1, 'Coach_ID', 'default')
Fox.insert(1, 'Coach_ID', 'default')

Paul.loc[Paul.Team_ID == 'HOU', 'Coach_ID'] = "M_D'Antoni"
Paul.loc[Paul.Team_ID == 'OKC', 'Coach_ID'] = "B_Donovan"
Paul.loc[Paul.Team_ID == 'PHO', 'Coach_ID'] = "M_Williams"

Walton_start_date = '2019-04-14'

Fox.loc[Fox.Date_ID >= Walton_start_date, 'Coach_ID'] = "L_Walton"
Fox.loc[Fox.Date_ID < Walton_start_date, 'Coach_ID'] = "D_Joerger"

### FACT TABLE 

new = pd.concat([Curry,Lillard,Mitchell,Paul,Fox])

new['Age']=new['Age'].astype(str).str[:2]
new['Age']=new['Age'].astype(int)

# Create SQL Table for Fact Table

sql_1 = "CREATE TABLE NBA_Fact_Table(Player_ID varchar(250), Team_ID varchar(250), Coach_ID varchar(250), Age int(4), Date_ID DATE, Opp varchar(250), Win_Loss varchar(250), Differential int(3), GP varchar(250), FG float(6,1), FGA float(6,1), FG_Percent float(4,3), 3P float(6,1), 3PA float(6,1), 3P_Percent float(4,3), FT float(6,1), FTA float(6,1), FT_Percent float(4,3), ORB float(3,1), DRB float(3,1), TRB float(3,1), AST float(3,1), STL float(3,1), BLK float(3,1), TOV float(3,1), PF float(3,1), PTS float(4,1), Plus_Minus float(3,1), Postseason varchar(5));" 
cursor.execute(sql_1)
connection.commit()


# DataFrame to SQL Table - append row by row 

column_list = "`,`".join([str(i) for i in new.columns.tolist()])

for i,row in new.iterrows():
    sql_2 = "INSERT INTO NBA_Fact_Table (`" +column_list + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql_2, tuple(row))
    connection.commit()

### COACH DIMENSION

coach_array=new['Coach_ID'].unique()
name_array = ['Steve Kerr', 'Terry Stotts','Quin Snyder',"Mike D'Antoni", 'Billy Donovan','Monty Williams','Dave Joerger','Luke Walton']
age_array = [55,63,54,69,55,49,47,40]
race_array = ['White','White','White','White','White','Black','White','White']
years_experience_array = [8,10,8,17,7,9,7,6]
played_array = [1,1,1,1,1,1,0,1]
coty_array = [1,0,0,1,0,0,0,0]
cand_array = [1,0,1,1,1,1,0,0]
college_array = ['Arizona','Oklahoma','Duke','Marshall','Providence','Notre Dame','Minnesota State-Moorhead','Arizona']



cols =['Coach_ID','Name','Age','Race', 'Years_Experience', 'Played_in_NBA','Won_COTY','COTY_Candidate','College']

Coach_Dimension = pd.DataFrame(columns = cols)


Coach_Dimension['Coach_ID'] = coach_array
Coach_Dimension['Name'] = name_array
Coach_Dimension['Age'] = age_array
Coach_Dimension['Race'] = race_array
Coach_Dimension['Years_Experience'] = years_experience_array 
Coach_Dimension['Played_in_NBA'] = played_array
Coach_Dimension['Won_COTY'] = coty_array
Coach_Dimension['COTY_Candidate'] = cand_array
Coach_Dimension['College'] = college_array

Coach_Dimension['Played_in_NBA'] = Coach_Dimension['Played_in_NBA'].astype(str)
Coach_Dimension['Won_COTY'] = Coach_Dimension['Won_COTY'].astype(str)
Coach_Dimension['COTY_Candidate'] = Coach_Dimension['COTY_Candidate'].astype(str)

Coach_Dimension['Played_in_NBA'] = Coach_Dimension['Played_in_NBA'].replace({'0':'No','1':'Yes'})
Coach_Dimension['Won_COTY'] = Coach_Dimension['Won_COTY'].replace({'0':'No','1':'Yes'})
Coach_Dimension['COTY_Candidate'] = Coach_Dimension['COTY_Candidate'].replace({'0':'No','1':'Yes'})

# Create SQL Table for Coach Dimension

sql_1 = "CREATE TABLE Coach_Dimension(Coach_ID varchar(250), Name varchar(250), Age int(4), Race varchar(250), Years_Experience int(3), Played_in_NBA varchar(20), Won_COTY varchar(20), COTY_Candidate varchar(20), College varchar(250));"           
cursor.execute(sql_1)
connection.commit()


# DataFrame to SQL Table - append row by row 

column_list = "`,`".join([str(i) for i in Coach_Dimension.columns.tolist()])

for i,row in Coach_Dimension.iterrows():
    sql_2 = "INSERT INTO Coach_Dimension (`" +column_list + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql_2, tuple(row))
    connection.commit()



### TEAM DIMENSION

cols =['Team_ID','Name','City','State', 'Arena_Name', 'Owner','Won_Championship','Reached_Finals']

Team_Dimension = pd.DataFrame(columns = cols)

team_array=new['Team_ID'].unique()
tname_array =['Golden State Warriors','Portland Trail Blazers','Utah Jazz','Houston Rockets','Oklahoma City Thunder','Phoenix Suns','Sacramento Kings'] 
city_array =['Oakland','Portland','Salt Lake City','Houston','Oklahoma City','Phoenix','Sacramento'] 
state_array =['California','Oregon','Utah','Texas','Oklahoma','Arizona','California']
arena_array = ['Chase Center','Moda Center','Vivint Arena','Toyota Center','Chesapeake Energy Arena','Phoenix Suns Arena','Golden 1 Center']
owner_array = ['Joseph Lacob','Paul Allen','Ryan Smith', 'Tilman Fertitta','Clay Bennett','Robert Sarver','Vivek Ranadivé']
won_chip_array = ['Yes','Yes','No','Yes','No','No','Yes']
finals_array = ['Yes','Yes','Yes','Yes','Yes','Yes','Yes']


Team_Dimension['Team_ID'] = team_array
Team_Dimension['Name'] = tname_array
Team_Dimension['City'] = city_array
Team_Dimension['State'] = state_array
Team_Dimension['Arena_Name'] = arena_array 
Team_Dimension['Owner'] = owner_array
Team_Dimension['Won_Championship'] = won_chip_array
Team_Dimension['Reached_Finals'] = finals_array


# Create SQL Table for Team Dimension

sql_1 = "CREATE TABLE Team_Dimension(Team_ID varchar(250), Name varchar(250), City varchar(250), State varchar(250), Arena_Name varchar(250), Owner varchar(250), Won_Championship varchar(250), Reached_Finals varchar(250));"           
cursor.execute(sql_1)
connection.commit()


# DataFrame to SQL Table - append row by row 

column_list = "`,`".join([str(i) for i in Team_Dimension.columns.tolist()])

for i,row in Team_Dimension.iterrows():
    sql_2 = "INSERT INTO Team_Dimension (`" +column_list + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql_2, tuple(row))
    connection.commit()


### DATE DIMENSION

cols = ['Date_ID','Month','Year']

Date_Dimension = pd.DataFrame(columns = cols)

date_array=new['Date_ID'].unique()

Date_Dimension['Date_ID'] = date_array
Date_Dimension['Year'] = pd.DatetimeIndex(Date_Dimension['Date_ID']).year
Date_Dimension['Month'] = pd.DatetimeIndex(Date_Dimension['Date_ID']).month

Date_Dimension['Month'] = Date_Dimension['Month'].astype(str)
Date_Dimension['Month'] = Date_Dimension['Month'].replace({'1':'Jan','2':'Feb','3':'Mar','4':'Apr','5':'May','6':'Jun','7':'Jul','8':'Aug','9':'Sep','10':'Oct','11':'Nov','12':'Dec',})

# Create SQL Table for Date Dimension

sql_1 = "CREATE TABLE Date_Dimension(Date_ID DATE, Month varchar(4), Year int(4));" 
cursor.execute(sql_1)
connection.commit()


# DataFrame to SQL Table - append row by row 

column_list = "`,`".join([str(i) for i in Date_Dimension.columns.tolist()])

for i,row in Date_Dimension.iterrows():
    sql_2 = "INSERT INTO Date_Dimension (`" +column_list + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql_2, tuple(row))
    connection.commit()


connection.close()
