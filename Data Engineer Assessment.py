import pandas as pd
import sqlite3
conn = sqlite3.connect('TestDB.db')
c = conn.cursor()
f_name=input("Enter file name with ext : ")
def read_txt():
    data_1=pd.read_csv(f_name,error_bad_lines=False,header=None,sep='|')
    data_1.drop(columns=[0,1], inplace=True)
    data_1.columns=data_1.iloc[0]
    data_1.drop(index=[0],inplace=True)
    data_1= data_1.reset_index(drop=True)
    return (data_1)
    
def filter_data(cn,m_data):
    fil=m_data['Country']==cn
    new_df=m_data[fil]
    #print("filter data for ",cn ," ",new_df)
    return(new_df)
    
updated_data=read_txt()
list_1=list(set(updated_data['Country']))
for x in list_1:
    tab_name=("client_name_"+ x)
    fil_data=filter_data(x,updated_data)
    fil_data.to_sql(tab_name, conn, if_exists='append', index = False) 
    
#data check
c.execute('''select * from client_name_IND''')
result=c.fetchall()
print(result)
conn.commit()