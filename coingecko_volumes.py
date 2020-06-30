#Made by Luca Santarella, code used for the analysis of cryptocurrency exchanges for my Bachelor thesis in Computer Science @ Università di Pisa
import os.path
import scrapy
import json

#defining subclass ScrapingSpider redefining attrs and methods

class ScrapingSpider(scrapy.Spider): 
    name = "coingecko_volumes" #unique name of the spider
    
#must return one or more Requests where the spider should start crawling

    def start_requests(self):
        days = 31
        urls = []
        for i in range(1,days):
            urls.append('file:///C:/Users/lucas/exchange_volume/CoinGecko_'+str(i)+'-3-2020.html') #insert correct path or directly URL requested
        
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        exchange_name = "kraken"
        if os.path.exists(exchange_name+'.json'):
            with open(exchange_name+'.json','r') as json_file:
                json_obj_vol = json.load(json_file)
        else:
            with open(exchange_name+'.json','w+') as json_file:
                json_file.write('{}')
                json_obj_vol = json.load(json_file)
                
        day = response.url.split('/')[7]
        day = day.split('_')[1].replace('.html','')
        exchanges_names = response.xpath('//*[contains(@class,"pt-2 flex-column")]').css('a::text').getall() #get every name
        exchanges_volumes = response.xpath('//*[contains(@class,"trade-vol-amount text-right")]').css('div::text').getall() #get every volume
        del exchanges_volumes[1::2]
        length = len(exchanges_names)
        for i in range(length): 
            if exchange_name in exchanges_names[i].lower():
                json_obj_vol[day] = exchanges_volumes[i].rstrip("\u00a0USD").replace(".","")
        json_common_vol = json.dumps(json_obj_vol) #creating JSON String to write on file JSON
        filename = exchange_name+'.json'
        with open(filename, 'w+') as f:
            f.write(json_common_vol)
       
        