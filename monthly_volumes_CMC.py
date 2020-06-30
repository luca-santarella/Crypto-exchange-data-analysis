#Made by Luca Santarella, code used for the analysis of cryptocurrency exchanges for my Bachelor thesis in Computer Science @ Università di Pisa

import scrapy
import json

#defining subclass ScrapingSpider redefining attributes and methods

class ScrapingSpider(scrapy.Spider): 
    name = "monthly_volumes" #unique name of the spider
    
#must return one or more Requests where the spider should start crawling

    def start_requests(self):
        #generating the Request, callback defines which function call after with the Response
        yield scrapy.Request(url='file:///C:/Users/lucas/exchange_volume/CoinMarketCap_reported_29-2-2020.html', callback=self.parse) #insert correct path or directly URL requested

    #parsing on the Response
    def parse(self, response):
        empty_json_object = '{}'
        json_object = json.loads(empty_json_object) #empty JSON Object
        exchanges_names = response.xpath('//*[contains(@class,"sc-1jx94bq-0 dWwPjl")]').css('a::text').getall() #get every name
        for exchange in exchanges_names: 
            if "Cat" in exchange:
                exchange = exchange.replace("Cat.","Cat")
            if "." in exchange:
                exchange = exchange.replace(".","-")
            if " " in exchange:
                exchange = exchange.replace(" ","-")
            print(exchange)
            volumes = response.xpath('//a[@href="/exchanges/'+exchange.lower().rstrip("-")+'/#markets"]').css('a::text').getall()
            monthly_volume = volumes[2].replace("$","").replace(".","")
            map_volumes = {exchange: monthly_volume} #map {"key":value}
            json_object.update(map_volumes) 
        
        json_file = json.dumps(json_object) #creating JSON String to write on file JSON
        filename = 'monthly_volumes.json'
        with open(filename, 'w+') as f:
            f.write(json_file)
       