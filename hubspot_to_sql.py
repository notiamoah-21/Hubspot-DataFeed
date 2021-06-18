import requests 
import json 
import pandas as pd 
import pyodbc
import numpy as np


#%%
" Part One - Create page one request and append dictionary to empty list "

URL = #Confidential
r = requests.get(URL)
d = r.text
Parsed = json.loads(d)

All_Pages = []

All_Pages.append(Parsed['results'])

#%%
" Part Two - Loop through all other requests url and apend to lists " 

has_more = True 
while has_more:
    r = requests.get(URL + "&after=" + str(Parsed['paging']['next']['after']))
    d = r.text
    Parsed = json.loads(d)
    All_Pages.append(Parsed['results'])
    has_more = Parsed.get('paging')

#%%
"Part Three - Transfer the statements into the properties dictionary"

statements = ["archived", "createdAt", "id", "updatedAt"]
numbers = [0,1,2,3,4]
for number in numbers:
    for number2 in list(range(len(All_Pages[number]))):
        for statement in statements:
            All_Pages[number][number2]['properties'][statement] = All_Pages[number][number2][statement]
            
#%%            
"Part Four - Create list of dictionary for all pages of properties dictionaries"

List_Dict = []
numbers = [0,1,2,3,4]

for number in numbers:
    for number2 in list(range(len(All_Pages[number]))):
        List_Dict.append(All_Pages[number][number2]['properties'])


#%%
"Part Five - Create the pandas dataframe from the list of dictionaries" 

df = pd.DataFrame(List_Dict)


#%%
" Part Six - Create a connetion for SQL interaction "
def create_connection():
    
    global cnxn
    global cursor

    
    connection_string = #Confidential
    
    cnxn = pyodbc.connect(connection_string)
    cursor = cnxn.cursor()


create_connection()


#%%

" Part Seven - Create sql statement to insert dataframe into sql"

values = list(df.values.tolist())


sql_statement = "INSERT  INTO hubspot.companies "
sql_statement += "(annual_revenue, city, country, created_date, description, domain, hs_lastmodified_date, hs_object_id, industry, name, phone, type, archived, created_at, id, update_at) "
sql_statement += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getutcdate())"

for value in values:
    cursor.execute(sql_statement, value)

cnxn.commit()

#%%

" Part Eight - Test to check for any more new records from api results"
# Convert SQL Query (id, update_at) into a pandas dataframe 
sql_df = pd.read_sql("SELECT [id], [update_at] FROM [hubspot].[companies]", cnxn)

# Convert column type of the df to match with sql_df
df['id'] = pd.to_numeric(df['id'])
df['updatedAt'] = pd.to_datetime(df['updatedAt']).dt.tz_localize(None)
sql_df['update_at'] = pd.to_datetime(sql_df['update_at'])
print(df.dtypes)
print(sql_df.dtypes)


# Select rows from api results based on logic conditions of sql dataframe, instead of returning if statements
logic_one = df['id'] != sql_df['id']
logic_two = (df['id'] == sql_df['id']) & (df['updatedAt'] != sql_df['update_at'])
if logic_one.all():
    values.append(df.loc[df['id'] != sql_df['id']].values.tolist())
elif logic_two.all():
    values.append(df.loc[(df['id'] == sql_df['id']) & (df['updatedAt'] != sql_df['update_at'])].values.tolist())
else:
    print("There are no new records")
