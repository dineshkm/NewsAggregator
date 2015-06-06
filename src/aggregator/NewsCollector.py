import requests
import bs4
import threading
from time import sleep
import time
import datetime

class Collector(threading.Thread):
   
    def __init__(self,storage):
        threading.Thread.__init__(self)
        self.websites = {}
        self.websites['TechGig'] = 'http://www.techgig.com/tech-news/editors-pick'
        self.websites['TechCrunch'] = 'http://techcrunch.com/'
        self.storage = storage
    
    def run(self):
        while(True):
            for site in self.websites.keys():
                response = requests.get(self.websites[site])
                soup = bs4.BeautifulSoup(response.text)
                stories = {}
                if site == 'TechGig':
                    stories = self.processTechGig(soup)
                elif site == 'TechCrunch':
                    stories = self.processTechCrunch(soup)
               # print stories
                rows = {}
                site_entry= {}
                entry = {}
                for title in stories.keys():
                    try:
                        entry[stories[title][1]] = title
                        site_entry[title] = {'title':title,'link':stories[title][0], 'timestamp': stories[title][1]}
                    except Exception:
                        print ""
                key = site.encode("utf-8")      
                rows[key] = entry
                self.storage.storeNews(rows)
                self.storage.storeSiteNews(site_entry)
                
            sleep(10*60*1000) #resume after 10 min
            
    def getTimestamp(self,timestamp):
        if timestamp.find("hrs") > 0:
            hrs = (timestamp.partition("hrs")[0]).strip()
            return int(hrs) *60*60*1000        
        elif timestamp.find("day") > 0:
            days = (timestamp.partition("day")[0]).strip()
            return int(days)*24 *60*60*1000
        return 0
            
    def processTechGig(self,soup):
        print "Processing Tech Gig"
        links = soup.select('div.news_item div.news_ttl_buzz a[href^=http:]')   
        times = soup.select('div.news_item div.social_social_bx span.tds_sts')
        stories = {}
        currtime = '{0:.0f}'.format(time.time()*1000)
        i = 0
        for link in links:
            convertedTime = self.getTimestamp(times[i].text)
            timestamp = int(currtime) - convertedTime
            stories[link.text.encode("utf-8")]= [link['href'].encode("utf-8"), str(timestamp)]
            i = i + 1
           # print link.text.encode("utf-8")
        return stories
    
    def processTechCrunch(self,soup):
        print "Processing Tech Crunch"
        links = soup.select('div.block-content a[href^=http:]')
        times = soup.select('div.block-content div.byline time.timestamp')
       
        stories = {}
        i = 0
        for link in links:
            if not link.has_attr('class'): 
                if i < len(times) and times[i].has_attr('datetime'):         
                    date= times[i]['datetime']
                    timestamp = time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timetuple())                        
                    stories[link.text.encode("utf-8")]= [link['href'].encode("utf-8"),str(timestamp)]
                    i = i + 1
            #    print link.text
        return stories
        
        
        
    
