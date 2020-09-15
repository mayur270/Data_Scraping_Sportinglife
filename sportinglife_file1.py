# -*- coding: utf-8 -*-

############################################## LIBRARIES ################################################

from requests import get
from bs4 import BeautifulSoup
from dateutil.parser import parse
from sqlalchemy import create_engine
import sqlalchemy as sql
import pandas as pd
import getpass
import re
import mysql.connector

########################################### DATES FOR URL ###############################################
#Choose Date Range for URL
#DATE_FORMAT(YYYY-MM-DD)
date_from = '2020-01-01'
date_to = '2020-01-01'

dates = []
for d in pd.date_range(date_from, date_to):
    de = d.strftime('%Y-%m-%d')
    dates.append(de)

################################## LOOPING DATES TO GET MULTIPLE URLs ####################################

#Concatenates all the UK horse racing events
uk_racecourse = ['aintree', 'ascot', 'ayr',
                 'bangor', 'bath', 'beverley',
                 'brighton','carlisle', 'cartmel',
                 'catterick', 'catterick-bridge',
                 'elmsford-city', 'cheltenham',
                 'epstow','chester', 'doncaster',
                 'epsom-downs', 'exeter', 'fakenham',
                 'ffos-las', 'fontwell', 'goodwood',
                 'hamilton', 'haydock', 'hereford',
                 'hexham', 'huntingdon', 'kelso',
                 'kempton', 'leicester', 'lingfield',
                 'ludlow', 'market-rasen', 'musselburgh',
                 'newbury', 'newcastle', 'newmarket',
                 'newton-abbot', 'nottingham', 'perth',
                 'plumpton', 'pontefract', 'redcar',
                 'ripon', 'salisbury', 'sandown',
                 'sedgefield', 'southwell', 'stratford-on-avon',
                 'taunton', 'thirsk', 'towcester',
                 'uttoxeter', 'warwick', 'wetherby',
                 'wincanton', 'windsor', 'wolverhampton',
                 'worcester', 'yarmouth', 'york']

todays_racecourse_names1=[]
racecourse_today=[]
links=[]
for date in dates:
    url = 'https://www.sportinglife.com/racing/results/%s' % (date)
    response = get(url)
    html_soup = BeautifulSoup(response.text, 'html.parser')

    sporting_life = html_soup.find_all('section', class_="hr-meeting-container")
    for each_racecourse_name in sporting_life:
        todays_racecourse_names = each_racecourse_name.find('div', class_="divider-title-text").span.a.text
        todays_racecourse_names1.append(todays_racecourse_names)

    for container in todays_racecourse_names1:
        if container.lower() in uk_racecourse:
            racecourse_today.append(container.lower())

    for racecourse in racecourse_today:
        sporting_links = html_soup.find_all('section', {"class":"hr-meeting-container", "id":str(racecourse)})
        for container in sporting_links:
            for a in container.find_all('li'):
                for link in a.find_all('a'):
                    link_href = link.get('href')
                    link_href = link_href.replace('results/%s/%s' % (date,racecourse), 'racecards/%s/%s/racecard' % (date,racecourse))
                    links.append(link_href)

#Removes Duplicates
mylinks = list(dict.fromkeys(links))
for i in mylinks:
    if i[-13:] == '#video-player':
        mylinks.remove(str(i))
    else:
        pass

newdf1 = pd.DataFrame({'Website':mylinks})

########################################### DATA EXTRACTION ################################################

horse_links = []
jockey_links = []
trainer_links = []
heading_texts1 = []
heading_times1 = []
racecourse_locations = []
event_names1 = []
overview_race_details = []
horse_gate_numbers = []
horse_names1 = []
horse_headgears = []
horse_last_rans = []
horse_ages1 = []
Horse_5_infos = []
horse_betting_odds = []
horse_jockey_claims = []
horse_medications = []
horse_insights = []
horse_written_infos = []

for each_horse_link in newdf1['Website']:
    url1 = "http://www.sportinglife.com"
    concatenate_url = url1 + each_horse_link
    response = get(concatenate_url)
    hl_sp = BeautifulSoup(response.text, 'html.parser')

##################################### HORSE DATA - BOTTOM SECTION ######################################

    table2 = hl_sp.find_all('section', class_='hr-racing-runner-wrapper')

    for each_variable in table2:
        #Horse Gate Number
        if each_variable.find('span', class_='hr-racing-runner-stall-no') is not None:
            horse_gate_number = each_variable.find('span', class_='hr-racing-runner-stall-no').text
            horse_gate_number = str(horse_gate_number)[1:-1]
        else:
            horse_gate_number = ''
        horse_gate_numbers.append(horse_gate_number)

        #Horse Links
        horse_link = each_variable.find('span', class_='hr-racing-runner-horse-name').a['href']
        horse_links.append(horse_link)

        #Horse Name
        horse_name1 = each_variable.find('span', class_='hr-racing-runner-horse-name').a.text
        horse_names1.append(horse_name1)

        #Horse Last Ran
        if each_variable.find('sup', class_='hr-racing-runner-horse-last-ran') is not None:
            horse_last_ran = each_variable.find('sup', class_='hr-racing-runner-horse-last-ran').text
        else:
            horse_last_ran = ''
        horse_last_rans.append(horse_last_ran)

        #Head Gear
        if each_variable.find('sup', class_='hr-racing-runner-horse-headgear') is not None:
            horse_headgear = each_variable.find('sup', class_='hr-racing-runner-horse-headgear').text
        else:
            horse_headgear = ''
        horse_headgears.append(horse_headgear)

        #Horse Age
        if each_variable.find('div', class_='hr-racing-runner-horse-sub-info') is not None:
            horse_age1 = each_variable.find('div', class_='hr-racing-runner-horse-sub-info').span.text
            horse_age1 = horse_age1.replace('Age: ', '')
        else:
            horse_age1 = ''
        horse_ages1.append(horse_age1)

        #Horse_5_info: Age, weight, jockey name, trainer name, Official Rating, other information
        if each_variable.find('div', class_='hr-racing-runner-horse-sub-info') is not None:
            lines = [i.get_text() for i in each_variable.find_all('div', {'class' : 'hr-racing-runner-horse-sub-info'})]

            for i in lines:
                Horse_5_info = i.replace('|\xa0\xa0',' ')
                Horse_5_info = i.replace('\xa0\xa0',' ')
                Horse_5_info = i.replace('\xa0',' ')
        else:
            Horse_5_info = ''
        Horse_5_infos.append(Horse_5_info)


        #Jockey_links
        jockey_link = each_variable.find('div', class_='hr-racing-runner-horse-sub-info').a['href']
        jockey_links.append(jockey_link)

        #Trainer links
        if each_variable.find('div', class_='hr-racing-runner-horse-sub-info') is not None:
            for link in each_variable.find_all('a'):
               trainer_link = link.get('href')
        else:
            trainer_link = ''
        trainer_links.append(trainer_link)

        #Betting Odds
        if each_variable.find('span', class_='hr-racing-runner-betting-link sui-odds') is not None:
            horse_betting_odd = each_variable.find('span', class_='hr-racing-runner-betting-link sui-odds').text
        else:
            horse_betting_odd = ''
        horse_betting_odds.append(horse_betting_odd)

        #Horse_Medication
        if each_variable.find('span', class_='hr-racing-runner-race-medication-stats') is not None:
            horse_medication = each_variable.find('span', class_='hr-racing-runner-race-medication-stats').text
        else:
            horse_medication = ''
        horse_medications.append(horse_medication)

        #Horse Race Insight
        if each_variable.find('div', class_='hr-insight-list') is not None:
            horse_insight = each_variable.find('div', class_='hr-insight-list').ul.text
        else:
            horse_insight = ''
        horse_insights.append(horse_insight)

        #Horse Jockey Claim
        if each_variable.find('span', class_='hr-racing-runner-form-jockey-claim') is not None:
            horse_jockey_claim = each_variable.find('span', class_='hr-racing-runner-form-jockey-claim').text
            horse_jockey_claim = horse_jockey_claim.replace(' ','')
            horse_jockey_claim = horse_jockey_claim[1:-1]
        else:
            horse_jockey_claim = ''
        horse_jockey_claims.append(horse_jockey_claim)

        #Horse Written Information
        if each_variable.find('p', class_='hr-racing-runner-form-watch-info-full') is not None:
            horse_written_info = each_variable.find('p', class_='hr-racing-runner-form-watch-info-full').text
        else:
            horse_written_info = ''
        horse_written_infos.append(horse_written_info)



############################################ TOP SECTION EXTRACTION ###########################################

        #Date of Race
        if hl_sp.find('h1', class_='page-main-subtitle') is not None:
            heading_text = hl_sp.find('h1', class_='page-main-subtitle').text
            heading_text1 = parse(heading_text).strftime("%Y-%m-%d")
        else:
            heading_text1 = ''
        heading_texts1.append(heading_text1)

        #Getting Time of Race
        if hl_sp.find('h1', class_='page-main-title') is not None:
            heading_time = hl_sp.find('h1', class_='page-main-title').text
            heading_time1 = heading_time[:5]
        else:
            heading_time1 = ''
        heading_times1.append(heading_time1)

        #Getting Racecourse Name/ Place
        if hl_sp.find('h1', class_='page-main-title') is not None:
            heading_time2 = heading_time[6:].split(' ')
            racecourse_location = str(heading_time2[:1])[2:-2]
        else:
            racecourse_location = ''
        racecourse_locations.append(racecourse_location)

        #Event Name
        if hl_sp.find('h1', class_="hr-racecard-race-summary-header") is not None:
            event_name1 = hl_sp.find('h1', class_="hr-racecard-race-summary-header").text
        else:
            event_name1 = ''
        event_names1.append(event_name1)


        #Getting Class, Race Length, Race Condition, Total Runners, Horse Track
        if hl_sp.find('h1', class_="hr-racecard-race-summary-info-text") is not None:
            overview_race_detail = hl_sp.find('h1', class_="hr-racecard-race-summary-info-text").text
            overview_race_detail = overview_race_detail.replace('\xa0\xa0','')
        else:
            overview_race_details = ''
        overview_race_details.append(overview_race_detail)

############################################ DATAFRAME ##################################################

horse_racing_df = pd.DataFrame({'Horse_link': horse_links,
                                     'Jockey_link':jockey_links,
                                     'Trainer_link':trainer_links,
                                     'Date':heading_texts1,
                                     'Time_Of_Event':heading_times1,
                                     'Location': racecourse_locations,
                                     'Name_of_Event': event_names1,
                                     'Overview_Race_Details': overview_race_details,
                                     'Horse_Gate':horse_gate_numbers,
                                     'Horse_Name':horse_names1,
                                     'Head_Gear':horse_headgears,
                                     'Last_Ran':horse_last_rans,
                                     'Age':horse_ages1,
                                     'Horse_Info':Horse_5_infos,
                                     'Betting_Odds':horse_betting_odds,
                                     'Jockey_Claims':horse_jockey_claims,
                                     'Surgery_or_Medication':horse_medications,
                                     'Horse_Insights':horse_insights,
                                     'Written_Info':horse_written_infos,
                                     })

####################################### EXPORTING TO MYSQL ##################################################

password = getpass.getpass('password:')

db_url1 = {
    'drivername': 'mysql',
    'username': '', # --> Add your username
    'password': password,
    'database':'dbname',
    'table':'horse_racing',
    'host': '127.0.0.1',
    'query': {'charset': 'utf8'},
}

mydb = mysql.connector.connect(
  host= db_url1['host'],
  user= db_url1['username'],
  passwd= db_url1['password'],
)
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS %s;" % db_url1['database'])
mycursor.execute("USE %s;\
                 CREATE TABLE IF NOT EXISTS %s( \
                id INT NOT NULL AUTO_INCREMENT, \
                PRIMARY KEY (id), \
                Horse_link CHAR(255) NOT NULL,\
                Jockey_link CHAR(255) NOT NULL,\
                Trainer_link CHAR(255) NOT NULL,\
                Date CHAR(30) NOT NULL, \
                Time_Of_Event TIME,\
                Location VARCHAR(30) NOT NULL,\
                Name_of_Event VARCHAR(100) NOT NULL, \
                Overview_Race_Details CHAR(255) NOT NULL,\
                Horse_Gate VARCHAR(30) NOT NULL, \
                Horse_Name VARCHAR(255) NOT NULL, \
                Head_Gear VARCHAR(30) NOT NULL, \
                Last_Ran VARCHAR(30) NOT NULL, \
                Age VARCHAR(30) NOT NULL, \
                Horse_Info VARCHAR(255) NOT NULL, \
                Betting_Odds VARCHAR(30) NOT NULL, \
                Jockey_Claims VARCHAR(30) NOT NULL, \
                Surgery_or_Medication VARCHAR(30) NOT NULL, \
                Horse_Insights VARCHAR(100) NOT NULL, \
                Written_Info TEXT \
                );" % (db_url1['database'],db_url1['table']))
mydb.close()

connect_string = 'mysql://%s:%s@%s/%s' % (db_url1['username'],db_url1['password'],db_url1['host'],db_url1['database'])
sql_engine = sql.create_engine(connect_string,  encoding="utf8")
con = sql_engine.connect()
horse_racing_df.to_sql(name=db_url1['table'],con=con,if_exists='append',index=False)
con.close()
