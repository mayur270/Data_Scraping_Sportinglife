# -*- coding: utf-8 -*-

############################################## LIBRARIES ################################################

from requests import get
from bs4 import BeautifulSoup
from dateutil.parser import parse
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import pandas as pd
import getpass
import re

########################################### DATES FOR URL ###############################################
#Choose Date Range for URL
#DATE_FORMAT(YYYY-MM-DD)
date1 = '2019-01-01'
date2 = '2019-01-01' 
   
dates =[]
for d in pd.date_range(date1, date2):
    de = d.strftime('%Y-%m-%d')
    dates.append(de)

################################## LOOPING DATES TO GET MULTIPLE URLs ####################################

links=[]
for date in dates:
    url = 'https://www.sportinglife.com/racing/racecards/%s' % (date)
    response = get(url)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    sporting_life = html_soup.find_all('section', class_='hr-meeting-container')
    for container in sporting_life:
        #for a in container.find_all('div', class_='hr-meeting-race-result-fulllink'):
        for a in container.find_all('li'):
            for link in a.find_all('a'):
                links.append(link.get('href'))

    test_df1 = pd.DataFrame({'Website':links})
    
    #Deletes all result links
    newdf= test_df1[~test_df1.Website.str.contains('/racing/results', na=False)]
    
    #Concatenates all the UK horse racing events
    uk_racecourse = ['aintree',	'ascot',	'ayr',	'bangor',	'bath',	'beverley',	'brighton',	
                 'carlisle', 'cartmel',	'catterick-bridge',	'chelmsford-city',	'cheltenham',
                 'epstow',	'chester',	'doncaster',	'epsom-downs',	'exeter',	'fakenham',	'ffos-las',	
                 'fontwell',	'goodwood',	'hamilton',	'haydock',	'hereford',	'hexham',	
                 'huntingdon',	'kelso',	'kempton',	'leicester',	'lingfield',	'ludlow',	'market-rasen',	
                 'musselburgh',	'newbury',	'newcastle',	'newmarket',	'newton-abbot',	'nottingham',	'perth',	
                 'plumpton',	'pontefract',	'redcar',	'ripon',	'salisbury',	'sandown',	'sedgefield',	
                 'southwell',	'stratford-on-avon',	'taunton',	'thirsk',	'towcester',	'uttoxeter',	'warwick',	
                 'wetherby',	'wincanton',	'windsor',	'wolverhampton',	'worcester',	'yarmouth',	'york']
    
    newdf1 = newdf[newdf['Website'].str.contains('|'.join(uk_racecourse), na=False)]
 
########################################### DATA EXTRACTION ################################################     

horse_links = []
heading_texts1 = []
heading_times1 = []
racecourse_locations = []
event_names1 = []
no_of_runners2 = []
surface_types1 = []
winning_times1 = []
horse_gate_numbers = []
horse_names1 = []
horse_headgears = []
horse_last_rans = []
horse_ages1 = []
horse_previous1 = []
horse_weights = []
horse_official_ratings = []
horse_medications = []
horse_insights = []
horse_betting_odds = []
horse_trainers= []
horse_jockey_names = []
horse_jockey_claims = []
horse_written_infos = []

for each_horse_link in newdf1['Website']:
    url5 = "http://www.sportinglife.com"
    conc_url10 = url5 + each_horse_link
    response5 = get(conc_url10)
    hl_sp = BeautifulSoup(response5.text, 'html.parser')

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
         
        #Head Gear - If/Esle Statement because sometimes its present in HTML Structure
        if each_variable.find('sup', class_='hr-racing-runner-horse-headgear') is not None:
            horse_headgear = each_variable.find('sup', class_='hr-racing-runner-horse-headgear').text
        else:
            horse_headgear = ''
        horse_headgears.append(horse_headgear)
        
        #Horse Last Ran
        if each_variable.find('sup', class_='hr-racing-runner-horse-last-ran') is not None:
            horse_last_ran = each_variable.find('sup', class_='hr-racing-runner-horse-last-ran').text
        else:
            horse_last_ran = ''
        horse_last_rans.append(horse_last_ran)
        
        #Horse Age
        if each_variable.find('div', class_='hr-racing-runner-horse-sub-info') is not None:
            horse_age1 = each_variable.find('div', class_='hr-racing-runner-horse-sub-info').span.text
        else:
            horse_age1 = ''
        horse_ages1.append(horse_age1)
 
        #Horse_Medication
        if each_variable.find('span', class_='hr-racing-runner-race-medication-stats') is not None:
            horse_medication = each_variable.find('span', class_='hr-racing-runner-race-medication-stats').text
        else:
            horse_medication = ''
        horse_medications.append(horse_medication)
    
        #Horse Race History Stats Summary
        if each_variable.find('span', class_='hr-racing-runner-race-history-stats') is not None:
            horse_previous = each_variable.find('span', class_='hr-racing-runner-race-history-stats').text
            
            horse_pre = each_variable.find('div', class_='hr-racing-runner-horse-sub-info').text
            horse_pre = horse_pre[len(horse_age1):]
            horse_pre = horse_pre.replace(str(horse_previous), '')
            horse_pre = horse_pre.split('OR:')[1:]
            horse_pre = str(horse_pre).replace(horse_medication,'')
            horse_pre = str(horse_pre).replace(' ','')
            horse_pre = ''.join(filter(str.isalpha, horse_pre))
            
            if len(str(horse_pre)) >= 1:
                horse_previous = horse_previous + ',' + horse_pre  
        else:
            horse_previous = ''
        horse_previous1.append(horse_previous)
 
        #Horse Weight
        if each_variable.find('div', class_='hr-racing-runner-horse-sub-info') is not None:            
            horse_weight = each_variable.find('div', class_='hr-racing-runner-horse-sub-info').text
            horse_weight = horse_weight[len(horse_age1):]
            horse_weight = horse_weight.replace(str(horse_previous), '')
            horse_weight = horse_weight.split('OR:')[:1]
            horse_weight = horse_weight[:1]
            horse_weight = str(horse_weight)[2:-2]
        else:
            horse_weight = ''
        horse_weights.append(horse_weight)
           
        #Official Rating(OR)
        if each_variable.find('div', class_='hr-racing-runner-horse-sub-info') is not None:          
            horse_official_rating = each_variable.find('div', class_='hr-racing-runner-horse-sub-info').text
            horse_official_rating = horse_official_rating[len(horse_age1):]
            horse_official_rating = horse_official_rating.replace(str(horse_previous), '')
            horse_official_rating = horse_official_rating.split('OR:')[1:]
            horse_official_rating = re.findall('\\d+', str(horse_official_rating))
            horse_official_rating = str(horse_official_rating)[2:-2]
        else:
            horse_official_rating = ''
        horse_official_ratings.append(horse_official_rating)    

        #Horse Race Insight
        if each_variable.find('div', class_='hr-insight-list') is not None:
            horse_insight = each_variable.find('div', class_='hr-insight-list').ul.text
        else:
            horse_insight = ''
        horse_insights.append(horse_insight)
        
        #Betting Odds
        if each_variable.find('span', class_='hr-racing-runner-betting-link sui-odds') is not None:
            horse_betting_odd = each_variable.find('span', class_='hr-racing-runner-betting-link sui-odds').text
        else:
            horse_betting_odd = ''
        horse_betting_odds.append(horse_betting_odd)
        
        #Horse Trainer
        if each_variable.find('a', class_='hr-racing-runner-form-trainer') is not None:
            horse_trainer = each_variable.find('a', class_='hr-racing-runner-form-trainer').span.text
        else:
            horse_trainer = ''
        horse_trainers.append(horse_trainer)
        
        #Horse Jockey Name
        if each_variable.find('span', class_='hr-racing-runner-form-jockey-name') is not None:
            horse_jockey_name = each_variable.find('span', class_='hr-racing-runner-form-jockey-name').text
        else:
            horse_jockey_name = ''
        horse_jockey_names.append(horse_jockey_name)
        
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
    
        #Finding Date
        if hl_sp.find('div', class_='hr-racing-racecard-heading-text') is not None:
            heading_text = hl_sp.find('div', class_='hr-racing-racecard-heading-text').h1.small.text
            heading_text1 = parse(heading_text).strftime("%Y-%m-%d")
        else:
            heading_text1 = ''
        heading_texts1.append(heading_text1)
        
        #Getting time from Top Section
        if hl_sp.find('div', class_='hr-racing-racecard-heading-text') is not None:
            heading_time = hl_sp.find('div', class_='hr-racing-racecard-heading-text').h1.text
            heading_time1 = heading_time[:5]
        else:
            heading_time1 = ''
        heading_times1.append(heading_time1)
        
        #Getting Racecourse Location from Top Section
        if hl_sp.find('div', class_='hr-racing-racecard-heading-text') is not None:
            heading_time2 = heading_time[6:].split(' ')
            racecourse_location = str(heading_time2[:1])[2:-2]
        else:
            racecourse_location = ''
        racecourse_locations.append(racecourse_location)
        
        #Event Name
        if hl_sp.find('li', class_='hr-racecard-summary-race-name hr-racecard-summary-always-open') is not None:
            event_name1 = hl_sp.find('li', class_='hr-racecard-summary-race-name hr-racecard-summary-always-open').text
        else:
            event_name1 = ''
        event_names1.append(event_name1)
        
        #No Of Runners
        if hl_sp.find('li', class_='hr-racecard-summary-race-runners hr-racecard-summary-always-open') is not None:
            no_of_runners = hl_sp.find('li', class_='hr-racecard-summary-race-runners hr-racecard-summary-always-open').text
            no_of_runners1 = no_of_runners.split(' ')
            no_of_runners1 = str(no_of_runners1[:1])[2:-2]
        else:
            no_of_runners1 = ''
        no_of_runners2.append(no_of_runners1)
        
        #Surface Type
        if hl_sp.find('li', class_='hr-racecard-summary-surface') is not None:
            surface_type = hl_sp.find('li', class_='hr-racecard-summary-surface').text
            surface_type1 = surface_type.split(' ')
            surface_type1 = str(surface_type1[1:])[2:-2]
        else:
            surface_type1 = ''
        surface_types1.append(surface_type1)
        
        #Winning Time
        if hl_sp.find('span', class_='hr-racecard-weighed-in-wt') is not None:
            winning_time = hl_sp.find('span', class_='hr-racecard-weighed-in-wt').text
            winning_time1 = winning_time.split(':')
            winning_time1 = str(winning_time1[1:])[2:-2]
        else:
            winning_time1 = ''
        winning_times1.append(winning_time1)
    
############################################ DATAFRAME ##################################################

horse_racing_test_df = pd.DataFrame({'Horse_link': horse_links,
                                     'Date':heading_texts1, 
                                     'Time_Of_Event':heading_times1,
                                     'Location': racecourse_locations,
                                     'Name_of_Event': event_names1, 
                                     'Total_Runners': no_of_runners2, 
                                     'Surface':surface_types1,
                                     'Winning_Time': winning_times1,
                                     'Horse_Gate':horse_gate_numbers, 
                                     'Horse_Name':horse_names1, 
                                     'Head_Gear':horse_headgears,
                                     'Last_Ran':horse_last_rans,
                                     'Age':horse_ages1,
                                     'History_Stat':horse_previous1,
                                     'Horse_Weights':horse_weights, 
                                     'Official_Ratings':horse_official_ratings,
                                     'Betting_Odds':horse_betting_odds,
                                     'Trainer_Names':horse_trainers,
                                     'Jockey_Names':horse_jockey_names,
                                     'Jockey_Claims':horse_jockey_claims,
                                     'Written_Info':horse_written_infos, 
                                     'Surgery_or_Medication':horse_medications,
                                     'Horse_Insights':horse_insights
                                     })

####################################### EXPORTING TO MYSQL ##################################################

password = getpass.getpass('password:')

db_url = {
    'database': "dbname",
    'drivername': 'mysql',
    'username': 'root',
    'password': password,
    'host': '127.0.0.1',
    'query': {'charset': 'utf8'},  # the key-point setting
}

engine = create_engine(URL(**db_url), encoding="utf8")
con = engine.connect()
horse_racing_test_df.to_sql(name='horse_racing_test',con=con,if_exists='append',index=False)
con.close()

