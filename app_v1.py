# -*- coding: utf-8 -*-
"""
Created on Wed May 10 14:21:49 2023

@author: sivar
"""

# import mysql.connector

# # Establish a connection to the MySQL database
# cnx = mysql.connector.connect(user='root', password='Raji@19',
#                               host='localhost', database='PhonePe_Pulse')

# # Create a cursor object to execute queries
# cursor = cnx.cursor()

# # Run a query
# query = ("SHOW TABLES")
# cursor.execute(query)

# # Process the results
# for row in cursor:
#     print(row)

# # Close the cursor and connection
# cursor.close()
# cnx.close()
import mysql.connector
import streamlit as st 
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
def get_table_data(pd,cursor,query) :
    #Now need to get users information from database
    cursor.execute(query)
    # Fetch the results
    results = cursor.fetchall()
    # Convert the results to a Pandas DataFrame
    mtd_i = pd.DataFrame(results, columns=[i[0] for i in cursor.description])
    return mtd_i


#Reading the longitude & lattitude data
Scatter_Geo_Dataset =  pd.read_csv(r'../PhonePePulseData_test/data/Data_Map_Districts_Longitude_Latitude.csv')
Indian_States =  pd.read_csv(r'../PhonePePulseData_test/data/Longitude_Latitude_State_Table.csv')

cnx = mysql.connector.connect(user='root', password='Raji@19',
                              host='localhost', database='PhonePe_Pulse_db_test3')
c1,c2=st.columns(2)

with c1:
    Year = st.selectbox(
            'Please select the Year',
            ('2018', '2019', '2020','2021','2022'))
with c2:
    Quarter = st.selectbox(
            'Please select the Quarter',
            ('1', '2', '3','4'))
    
cursor = cnx.cursor()

# Run a query
# query = ("SHOW TABLES")
# cursor.execute(query)
# query = "SELECT * FROM map_transaction WHERE year = %s AND Quarter = %s"
# cursor.execute(query, (Year,Quarter))
# # Fetch the results
# results = cursor.fetchall()

# # Convert the results to a Pandas DataFrame
# mtd = pd.DataFrame(results, columns=[i[0] for i in cursor.description])
#Get map transaction data
query="SELECT * FROM map_user"
mud = get_table_data(pd,cursor, query)
query="SELECT * FROM map_transaction"
mtd = get_table_data(pd, cursor,query)
query="SELECT * FROM aggregated_transaction"
atd = get_table_data(pd, cursor, query)
query="SELECT * FROM aggregated_user"
aud = get_table_data(pd, cursor, query)
#Query for selected Year & Quarter

query="SELECT * FROM map_transaction WHERE Year = "+Year+" AND Quarter = "+Quarter+" AND State != 'india'"
mtd_yq=get_table_data(pd,cursor,query)


#Now we calculate total count & amount values of each state as a sum of all districts
#smtd = pd.DataFrame(columns=mtd.columns)
query="SELECT * FROM map_transaction WHERE Year = "+Year+" AND Quarter = "+Quarter+" AND State = 'india'"
mtd_yq_sind=get_table_data(pd,cursor,query)

# Dynamic Scattergeo Data Generation
mtd_yq = mtd_yq.sort_values(by=['district_name'], ascending=False)
Scatter_Geo_Dataset = Scatter_Geo_Dataset.sort_values(by=['District'], ascending=False) 

#This is to get if any districts are missing compared to the geo dataset
extra_districts = Scatter_Geo_Dataset[~Scatter_Geo_Dataset['District'].isin(mtd_yq['district_name'])]['District']

#Adding missing rows
for d in extra_districts :
        new_row_index = len(mtd_yq)
        mtd_yq.loc[new_row_index] = None
        mtd_yq.loc[new_row_index, 'Year'] = Year
        mtd_yq.loc[new_row_index,'Quarter'] = Quarter
        mtd_yq.loc[new_row_index,'district_name'] = d
        mtd_yq.loc[new_row_index, 'HoverDataMetric_type']  = 'TOTAL'
        mtd_yq.loc[new_row_index,'HoverDataMetric_count'] = 0
        mtd_yq.loc[new_row_index,'HoverDataMetric_amount'] = 0.0
        matching_index = Scatter_Geo_Dataset.loc[Scatter_Geo_Dataset['District'] == d].index
        mtd_yq.loc[new_row_index, 'State'] = Scatter_Geo_Dataset.loc[matching_index[0],'State']
        # matching_index = Scatter_Geo_Dataset.loc[Scatter_Geo_Dataset['District'] == d].index
        # mtd.loc[new_row_index, 'State'] = Scatter_Geo_Dataset.loc[matching_index, Scatter_Geo_Dataset.columns.get_loc('State')]

Total_Amount=[]
for i in mtd_yq['HoverDataMetric_amount']:
    Total_Amount.append(i)
Scatter_Geo_Dataset['Total_Amount']=Total_Amount

Total_Transaction=[]
for i in mtd_yq['HoverDataMetric_count']:
    Total_Transaction.append(i)
Scatter_Geo_Dataset['Total_Transactions']=Total_Transaction
Scatter_Geo_Dataset['Year_Quarter']=str(Year)+'-Q'+str(Quarter)

# mtd_yq_sind=mtd_yq[mtd_yq["State"] == "india"]
# mtd_yq.drop(mtd_yq.index[(mtd_yq["State"] == "india")],axis=0,inplace=True)



#Now we calculate the total users count per state
# umud = pd.DataFrame(columns=mud.columns)


#smtd = smtd.sort_values(by=['district_name'], ascending=False)
query="SELECT * FROM map_user WHERE Year = "+Year+" AND Quarter ="+Quarter+" AND State != 'india'"
mud_yq = get_table_data(pd, cursor,query)

query="SELECT * FROM map_user WHERE Year = "+Year+" AND Quarter ="+Quarter+" AND State = 'india'"
mud_yq_sind = get_table_data(pd, cursor,query)
# Dynamic Coropleth
mud_yq_sind = mud_yq_sind.sort_values(by=['district_name'], ascending=False)
mtd_yq_sind = mtd_yq_sind.sort_values(by=['district_name'], ascending=False)
#mud_yq = mud_yq.sort_values(by=['State'], ascending=False)
#mtd_yq = mtd_yq.sort_values(by=['State'], ascending=False)
Total_Amount=[]
for i in mtd_yq_sind['HoverDataMetric_amount']:
    Total_Amount.append(i)
mud_yq_sind['Total_Amount']=Total_Amount
Total_Transaction=[]
for i in mtd_yq_sind['HoverDataMetric_count']:
    Total_Transaction.append(i)
mud_yq_sind['Total_Transactions']=Total_Transaction

# # -------------------------------------FIGURE1 INDIA MAP------------------------------------------------------------------
# #scatter plotting the states codes 
Indian_States = Indian_States.sort_values(by=['state'], ascending=False)
Indian_States['Registered_Users']=mud_yq_sind['Registered_users']
Indian_States['Total_Amount']=mud_yq_sind['Total_Amount']
Indian_States['Total_Transactions']=mud_yq_sind['Total_Transactions']
Indian_States['Year_Quarter']=str(Year)+'-Q'+str(Quarter)
fig=px.scatter_geo(Indian_States,
                    lon=Indian_States['Longitude'],
                    lat=Indian_States['Latitude'],                                
                    text = Indian_States['code'], #It will display district names on map
                    hover_name="state", 
                    hover_data=['Total_Amount',"Total_Transactions","Year_Quarter"],
                    )
fig.update_traces(marker=dict(color="white" ,size=0.3))
fig.update_geos(fitbounds="locations", visible=False,)
    # scatter plotting districts
Scatter_Geo_Dataset['col']=Scatter_Geo_Dataset['Total_Transactions']
fig1=px.scatter_geo(Scatter_Geo_Dataset,
                    lon=Scatter_Geo_Dataset['Longitude'],
                    lat=Scatter_Geo_Dataset['Latitude'],
                    color=Scatter_Geo_Dataset['col'],
                    size=Scatter_Geo_Dataset['Total_Transactions'],     
                    #text = Scatter_Geo_Dataset['District'], #It will display district names on map
                    hover_name="District", 
                    hover_data=["State", "Total_Amount","Total_Transactions","Year_Quarter"],
                    title='District',
                    size_max=22,)
fig1.update_traces(marker=dict(color="rebeccapurple" ,line_width=1))    #rebeccapurple
#coropleth mapping india
fig_ch = px.choropleth(
                    mud_yq_sind,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',                
                    locations='district_name',
                    color="Total_Transactions",                                       
                    )
fig_ch.update_geos(fitbounds="locations", visible=False,)
#combining districts states and coropleth
fig_ch.add_trace( fig.data[0])
fig_ch.add_trace(fig1.data[0])
st.write("### **:blue[PhonePe India Map]**")
colT1,colT2 = st.columns([6,4])
with colT1:
    st.plotly_chart(fig_ch, use_container_width=True)
with colT2:
    st.info(
    """
    Details of Map:
    - The darkness of the state color represents the total transactions
    - The Size of the Circles represents the total transactions dictrict wise
    - The bigger the Circle the higher the transactions
    - Hover data will show the details like Total transactions, Total amount
    """
    )
    st.info(
    """
    Important Observations:
    - User can observe Transactions of PhonePe in both statewide and Districtwide.
    - We can clearly see the states with highest transactions in the given year and quarter
    - We get basic idea about transactions district wide
    """
    )
# -----------------------------------------------FIGURE2 HIDDEN BARGRAPH------------------------------------------------------------------------
mud_yq_sind = mud_yq_sind.sort_values(by=['Total_Transactions'])
fig = px.bar(mud_yq_sind, x='district_name', y='Total_Transactions',title=str(Year)+" Quarter-"+str(Quarter))
with st.expander("See Bar graph for the same data"):
    st.plotly_chart(fig, use_container_width=True)
    st.info('**:blue[The above bar graph showing the increasing order of PhonePe Transactions according to the states of India, Here we can observe the top states with highest Transaction by looking at graph]**')

# # # #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ TRANSACTIONS ANALYSIS @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

st.write('# :green[TRANSACTIONS ANALYSIS :currency_exchange:]')
tab1, tab2, tab3, tab4 = st.tabs(["STATE ANALYSIS", "DISTRICT ANALYSIS", "YEAR ANALYSIS", "OVERALL ANALYSIS"])
# #==================================================T FIGURE1 STATE ANALYSIS=======================================================

with tab1:
    atd_local=atd.copy()
    # atd=Data_Aggregated_Transaction_df.copy()
    # atd.drop(atd.index[(atd["State"] == "india")],axis=0,inplace=True)
 
    st.write('### :green[State & PaymentMode]')
    col1, col2= st.columns(2)
    with col1:
        typelist = atd_local['Transaction_type'].unique()
        # mode = st.selectbox(
        #     'Please select the Mode',
        #     ('Recharge & bill payments', 'Peer-to-peer payments', 'Merchant payments', 'Financial Services','Others'),key='a')
        mode = st.selectbox(
            'Please select the Mode',
            typelist,key='a')
    with col2:
        statelist = atd_local['State'].unique()
        state = st.selectbox(
        'Please select the State',
        statelist,key='b')
    State= state
    Year_List=[2018,2019,2020,2021,2022]
    Mode=mode
    atd_local=atd.loc[(atd['State'] == State ) & (atd_local['Year'].isin(Year_List)) & 
                            (atd_local['Transaction_type']==Mode )]
    atd_local = atd_local.sort_values(by=['Year'])
    atd_local["Quarter"] = "Q"+atd_local['Quarter'].astype(str)
    atd_local["Year_Quarter"] = atd_local['Year'].astype(str) +"-"+ atd_local["Quarter"].astype(str)
    fig = px.bar(atd_local, x='Year_Quarter', y='Transaction_count',color="Transaction_count",
                  color_continuous_scale="Viridis")
    
    # State_PaymentMode=State_PaymentMode.loc[(State_PaymentMode['State'] == State ) & (State_PaymentMode['Year'].isin(Year_List)) & 
    #                         (State_PaymentMode['Payment_Mode']==Mode )]
    # State_PaymentMode = State_PaymentMode.sort_values(by=['Year'])
    # State_PaymentMode["Quarter"] = "Q"+State_PaymentMode['Quarter'].astype(str)
    # State_PaymentMode["Year_Quarter"] = State_PaymentMode['Year'].astype(str) +"-"+ State_PaymentMode["Quarter"].astype(str)
    # fig = px.bar(State_PaymentMode, x='Year_Quarter', y='Total_Transactions_count',color="Total_Transactions_count",
    #              color_continuous_scale="Viridis")
        
    colT1,colT2 = st.columns([7,3])
    with colT1:
        st.write('#### '+State.upper()) 
        st.plotly_chart(fig,use_container_width=True)
    with colT2:
        st.info(
        """
        Details of BarGraph:
        - This entire data belongs to state selected by you
        - X Axis is basically all years with all quarters 
        - Y Axis represents total transactions in selected mode        
        """
        )
        st.info(
        """
        Important Observations:
        - User can observe the pattern of payment modes in a State 
        - We get basic idea about which mode of payments are either increasing or decreasing in a state
        """
        )
# #=============================================T FIGURE2 DISTRICTS ANALYSIS=============================================
with tab2:
    mtd_local=mtd.copy()
    col1, col2, col3= st.columns(3)
    with col1:
        Year = st.selectbox(
            'Please select the Year',
            ('2018', '2019', '2020','2021','2022'),key='y1')
    with col2:
        statelist = mtd_local['State'].unique()
        state = st.selectbox(
        'Please select the State',
        statelist,key='dk')
    with col3:
        Quarter = st.selectbox(
            'Please select the Quarter',
            ('1', '2', '3','4'),key='qwe')
    districts=mtd_local.loc[(mtd['State'] == state ) & (mtd_local['Year']==int(Year))
                                          & (mtd_local['Quarter']==int(Quarter))]
    l=len(districts)    
    fig = px.bar(districts, x='district_name', y='HoverDataMetric_count',color="HoverDataMetric_count",
                  color_continuous_scale="Viridis")   
    colT1,colT2 = st.columns([7,3])
    with colT1:
        st.write('#### '+state.upper()+' WITH '+str(l)+' DISTRICTS')
        st.plotly_chart(fig,use_container_width=True)
    with colT2:
        st.info(
        """
        Details of BarGraph:
        - This entire data belongs to state selected by you
        - X Axis represents the districts of selected state
        - Y Axis represents total transactions        
        """
        )
        st.info(
        """
        Important Observations:
        - User can observe how transactions are happening in districts of a selected state 
        - We can observe the leading distric in a state 
        """
        )
# #=============================================T FIGURE3 YEAR ANALYSIS===================================================
with tab3:
    atd_local=atd.copy()
    #st.write('### :green[PaymentMode and Year]')
    col1, col2= st.columns(2)
    with col1:
        typelist = atd_local['Transaction_type'].unique()
        M = st.selectbox(
            'Please select the Mode',
            typelist,key='D')
    with col2:
        ylist = atd_local['Year'].unique()
        Y = st.selectbox(
        'Please select the Year',
        ylist,key='F')
    # a=atd.copy()
    Year=int(Y)
    Mode=M
    atd_local=atd_local.loc[(atd_local['Year']==Year) & 
                            (atd_local['Transaction_type']==Mode )]
    States_List=atd_local['State'].unique()
    State_groupby_YP=atd_local.groupby('State')
    Year_PaymentMode_Table=State_groupby_YP.sum()
    Year_PaymentMode_Table['states']=States_List
    del Year_PaymentMode_Table['Quarter'] # ylgnbu', 'ylorbr', 'ylorrd teal
    del Year_PaymentMode_Table['Year']
    Year_PaymentMode_Table = Year_PaymentMode_Table.sort_values(by=['Transaction_count'])
    fig2= px.bar(Year_PaymentMode_Table, x='states', y='Transaction_count',color="Transaction_count",
                color_continuous_scale="Viridis",)   
    colT1,colT2 = st.columns([7,3])
    with colT1:
        st.write('#### '+str(Year)+' DATA ANALYSIS')
        st.plotly_chart(fig2,use_container_width=True) 
    with colT2:
        st.info(
        """
        Details of BarGraph:
        - This entire data belongs to selected Year
        - X Axis is all the states in increasing order of Total transactions
        - Y Axis represents total transactions in selected mode        
        """
        )
        st.info(
        """
        Important Observations:
        - We can observe the leading state with highest transactions in particular mode
        - We get basic idea about regional performance of Phonepe
        - Depending on the regional performance Phonepe can provide offers to particular place
        """
        )
#=============================================T FIGURE4 OVERALL ANALYSIS=============================================
with tab4:    
    atd_local=atd.copy()
    years=atd_local.groupby('Year')
    years_List=atd_local['Year'].unique()
    years_Table=years.sum()
    del years_Table['Quarter']
    years_Table['year']=years_List
    total_trans=years_Table['Transaction_count'].sum() # this data is used in sidebar    
    fig1 = px.pie(years_Table, values='Transaction_count', names='year',color_discrete_sequence=px.colors.sequential.Viridis, title='TOTAL TRANSACTIONS (2018 TO 2022)')
    col1, col2= st.columns([0.65,0.35])
    with col1:
        st.write('### :green[Drastical Increase in Transactions :rocket:]')
        st.plotly_chart(fig1)
    with col2:  
        st.write('#### :green[Year Wise Transaction Analysis in INDIA]')      
        st.markdown(years_Table.style.hide(axis="index").to_html(), unsafe_allow_html=True)
        st.info(
        """
        Important Observations:
        - Its very clearly understood that online transactions drasticall increased
        - Initially in 2018,2019 the transactions are less but with time the online payments are increased at a high scale via PhonePe.
        - We can clearly see that more than 50% of total Phonepe transactions in india happened are from the year 2022
        """
        )

# # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ USER ANALYSIS @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

st.write('# :orange[USERS DATA ANALYSIS ]')
tab1, tab2, tab3, tab4 = st.tabs(["STATE ANALYSIS", "DISTRICT ANALYSIS","YEAR ANALYSIS","OVERALL ANALYSIS"])

# # =================================================U STATE ANALYSIS ========================================================
with tab1:
    st.write('### :blue[State & Userbase]')
    state = st.selectbox(
        'Please select the State',
        ('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
        'assam', 'bihar', 'chandigarh', 'chhattisgarh',
        'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
        'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
        'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep',
        'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram',
        'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan',
        'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
        'uttarakhand', 'west-bengal'),key='W')
    app_opening=mud.groupby(['State','Year'])
    a_state=app_opening.sum()
    la=mud['State'] +"-"+ mud["Year"].astype(str)
    a_state["state_year"] = la.unique()
    sta=a_state["state_year"].str[:-5]
    a_state["state"] = sta
    sout=a_state.loc[(a_state['state'] == state) ]
    ta=sout['Appopens'].sum()
    tr=sout['Registered_users'].sum()
    sout['Appopens']=sout['Appopens'].mul(100/ta)
    sout['Registered_users']=sout['Registered_users'].mul(100/tr).copy()
    fig = go.Figure(data=[
        go.Bar(name='Appopens %', y=sout['Appopens'], x=sout['state_year'], marker={'color': 'pink'}),
        go.Bar(name='Registered Users %', y=sout['Registered_users'], x=sout['state_year'],marker={'color': 'orange'})
    ])
    # Change the bar mode
    fig.update_layout(barmode='group')
    colT1,colT2 = st.columns([7,3])
    with colT1:
        st.write("#### ",state.upper())
        st.plotly_chart(fig, use_container_width=True, height=200)
    with colT2:
        st.info(
        """
        Details of BarGraph:
        - user need to select a state 
        - The X Axis shows both Registered users and App openings 
        - The Y Axis shows the Percentage of Registered users and App openings
        """
        )
        st.info(
        """
        Important Observations:
        - User can observe how the App Openings are growing and how Registered users are growing in a state
        - We can clearly obseve these two parameters with time
        - one can observe how user base is growing
        """
        )
# ==================================================U DISTRICT ANALYSIS ====================================================
with tab2:
     mud_local=mud.copy()
     col1, col2, col3= st.columns(3)
     with col1:
         ylist= mud_local['Year'].unique()
         Year = st.selectbox(
             'Please select the Year',
             ylist,key='y12')
     with col2:
         slist= mud_local['State'].unique()
         state = st.selectbox(
         'Please select the State',
         slist,key='dk2')
     with col3:
         Quarter = st.selectbox(
             'Please select the Quarter',
             ('1', '2', '3','4'),key='qwe2')
     districts=mud_local.loc[(mud_local['State'] == state ) & (mud_local['Year']==int(Year))
                                           & (mud_local['Quarter']==int(Quarter))]
     l=len(districts)    
     fig = px.bar(districts, x='district_name', y='Appopens',color="Appopens",
                   color_continuous_scale="reds")   
     colT1,colT2 = st.columns([7,3])
     with colT1:
         if l:
             st.write('#### '+state.upper()+' WITH '+str(l)+' DISTRICTS')
             st.plotly_chart(fig,use_container_width=True)
         else:
             st.write('#### NO DISTRICTS DATA AVAILABLE FOR '+state.upper())

     with colT2:
         if l:
             st.info(
         """
         Details of BarGraph:
         - This entire data belongs to state selected by you
         - X Axis represents the districts of selected state
         - Y Axis represents App Openings       
         """
             )
             st.info(
         """
         Important Observations:
         - User can observe how App Openings are happening in districts of a selected state 
         - We can observe the leading distric in a state 
         """
             )
# ==================================================U YEAR ANALYSIS ========================================================
with tab3:
    st.write('### :orange[Brand Share] ')
    aud_local=aud.copy()
    col1, col2= st.columns(2)
    with col1:
        slist= aud_local['State'].unique()
        state = st.selectbox(
        'Please select the State',
        slist,key='Z')
    with col2:
        ylist= aud_local['Year'].unique()
        Y = st.selectbox(
        'Please select the Year',
        ylist,key='X')
    y=int(Y)
    s=state
    brand=aud_local[aud_local['Year']==y] 
    brand=aud_local.loc[(aud_local['Year'] == y) & (aud_local['State'] ==s)]
    myb= brand['Brand_type'].unique()
    x = sorted(myb).copy()
    b=brand.groupby('Brand_type').sum()
    b['brand']=x
    br=b['Brand_count'].sum()
    labels = b['brand']
    values = b['Brand_count'] # customdata=labels,
    fig3 = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.4,textinfo='label+percent',texttemplate='%{label}<br>%{percent:1%f}',insidetextorientation='horizontal',textfont=dict(color='#000000'),marker_colors=px.colors.qualitative.Prism)])
  
    colT1,colT2 = st.columns([7,3])
    with colT1:
        st.write("#### ",state.upper()+' IN '+str(Y))
        st.plotly_chart(fig3, use_container_width=True)
    with colT2:
        st.info(
        """
        Details of Donut Chart:        
        - Initially we select data by means of State and Year
        - Percentage of registered users is represented with dounut chat through Device Brand
        """
        )
        st.info(
        """
        Important Observations:
        - User can observe the top leading brands in a particular state
        - Brands with less users
        - Brands with high users
        - Can make app download advices to growing brands
        """
        )

    b = b.sort_values(by=['Brand_count'])
    fig4= px.bar(b, x='brand', y='Brand_count',color="Brand_count",
                title='In '+state+'in '+str(y),
                color_continuous_scale="oranges",)
    with st.expander("See Bar graph for the same data"):
        st.plotly_chart(fig4,use_container_width=True) 
# ===================================================U OVERALL ANALYSIS ====================================================

    with tab4:
        mud_local=mud.copy()
        years=mud_local.groupby('Year')
        years_List=mud_local['Year'].unique()
        years_Table=years.sum()
        del years_Table['Quarter']
        years_Table['Year']=years_List
        total_trans=years_Table['Registered_users'].sum() # this data is used in sidebar    
        fig1 = px.pie(years_Table, values='Registered_users', names='Year',color_discrete_sequence=px.colors.sequential.RdBu, title='TOTAL REGISTERED USERS (2018 TO 2022)')
        col1, col2= st.columns([0.7,0.3])
        with col1:
            # st.write('### :green[Drastical Increase in Transactions :rocket:]')
            labels = ["US", "China", "European Union", "Russian Federation", "Brazil", "India",
                "Rest of World"]

            # Create subplots: use 'domain' type for Pie subplot
            fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'}, {'type':'domain'}]])
            fig.add_trace(go.Pie(labels=years_Table['Year'], values=years_Table['Registered_users'], name="REGISTERED USERS"),
                        1, 1)
            fig.add_trace(go.Pie(labels=years_Table['Year'], values=years_Table['Appopens'], name="APP OPENINGS"),
                        1, 2)

            # Use `hole` to create a donut-like pie chart
            fig.update_traces(hole=.6, hoverinfo="label+percent+name")

            fig.update_layout(
                title_text="USERS DATA (2018 TO 2022)",
                # Add annotations in the center of the donut pies.
                annotations=[dict(text='USERS', x=0.18, y=0.5, font_size=20, showarrow=False),
                            dict(text='APP', x=0.82, y=0.5, font_size=20, showarrow=False)])
            # st.plotly_chart(fig1)
            st.plotly_chart(fig)
        with col2:  
            # st.write('#### :green[Year Wise Transaction Analysis in INDIA]')      
            st.markdown(years_Table.style.hide(axis="index").to_html(), unsafe_allow_html=True)
            st.info(
            """
            Important Observation:
            -  We can see that the Registered Users and App openings are increasing year by year
          
            """
            )

st.write('# :red[TOP 3 STATES DATA]')
c1,c2=st.columns(2)
with c1:
    Year = st.selectbox(
            'Please select the Year',
            ('2022', '2021','2020','2019','2018'),key='y1h2k')
with c2:
    Quarter = st.selectbox(
            'Please select the Quarter',
            ('1', '2', '3','4'),key='qgwe2')
mud_local=mud.copy() 
mtd_local=mtd.copy() 
top_states=mud_local.loc[(mud_local['Year'] == int(Year)) & (mud_local['Quarter'] ==int(Quarter))]
top_states_r = top_states.sort_values(by=['Registered_users'], ascending=False)
top_states_a = top_states.sort_values(by=['Appopens'], ascending=False) 

top_states_T=mtd_local.loc[(mtd_local['Year'] == int(Year)) & (mtd_local['Quarter'] ==int(Quarter))]
topst=top_states_T.groupby('district_name')
x=topst.sum().sort_values(by=['HoverDataMetric_count'], ascending=False)
y=topst.sum().sort_values(by=['HoverDataMetric_amount'], ascending=False)
col1, col2, col3, col4= st.columns([2.5,2.5,2.5,2.5])
with col1:
    rt=top_states_r[1:4]
    st.markdown("#### :orange[Registered Users :bust_in_silhouette:]")
    st.markdown(rt[[ 'district_name','Registered_users']].style.hide(axis="index").to_html(), unsafe_allow_html=True)
with col2:
    at=top_states_a[1:4]
    st.markdown("#### :orange[PhonePeApp Openings:iphone:]")
    st.markdown(at[['district_name','Appopens']].style.hide(axis="index").to_html(), unsafe_allow_html=True)
with col3:
    st.markdown("#### :orange[Total Transactions:currency_exchange:]")
    st.write(x[['HoverDataMetric_count']][1:4])
with col4:
    st.markdown("#### :orange[Total Amount :dollar:]")
    st.write(y['HoverDataMetric_amount'][1:4])      
      

