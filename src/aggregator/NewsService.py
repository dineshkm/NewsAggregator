#Service layer to provide all service needs regarding news
class Service():   
    def __init__(self,storage):
        self.storage = storage
        
    #def fetchAllNews(self):
    
    def fecthSiteNews(self,siteName):
        titles = self.storage.getSiteNews(siteName)
        results = []
        for key in titles.keys():
            news = self.storage.getNews(titles[key])
            results.append(news)
        return results
            
            