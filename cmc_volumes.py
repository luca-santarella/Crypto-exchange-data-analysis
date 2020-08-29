#Made by Luca Santarella, code used for the analysis of cryptocurrency exchanges for my Bachelor thesis in Computer Science @ Università di Pisa
#Since the website layout of CoinMarketCap changed the following code could not be working anymore

import scrapy
import json
import os.path
#defining subclass ScrapingSpider redefining attrs and methods

class ScrapingSpider(scrapy.Spider): 
    name = "cmc_volumes" #unique name of the spider
    
#must return one or more Requests where the spider should start crawling

    def start_requests(self):
        days = 31
        urls = []
        for i in range(1,days):
            urls.append('file:///C:/Users/lucas/exchange_volume/CoinMarketCap__'+str(i)+'-3-2020.html') #insert correct path or directly URL requested
        
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    #parsing on the Response
    def parse(self, response):
        exchange_name = "kraken" #insert exchange desired
        if os.path.exists(exchange_name+'.json'):
            with open(exchange_name+'.json','r') as json_file:
                json_obj_vol = json.load(json_file)
        else:
            with open(exchange_name+'.json','w+') as json_file:
                json_file.write('{}')
                json_obj_vol = json.load(json_file)
                
        day = response.url.split('/')[7]
        day = day.split('_')[2].replace('.html','')
        volume = response.xpath('//a[@href="/exchanges/'+exchange_name.lower()+'/#markets"]').css('a::text').get() #extract volume
        json_obj_vol[day] = volume.strip("$").replace(".","") #format volume data
        json_file = json.dumps(json_obj_vol) #creating JSON String to write on file JSON
        filename = exchange_name+'.json'
        with open(filename, 'w+') as f:
            f.write(json_file)
        
