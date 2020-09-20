#This code will scrape exchange data from the CoinGecko website about 
#Luca Santarella

from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# add a delay so that all elements of 
# the webpage are loaded before proceeding 
import time  
import csv
import os.path
	  
# Creating a Chrome webdriver object 
driver = webdriver.Chrome(executable_path ="/bin/chromedriver")


def print_ex_features():
	
	#sleep 5 seconds to avoid accidentally accessing Cloudfare web page (DDoS control)
	time.sleep(5)
	#find the rows of the table presented on the web page
	elem_rows = []
	try:
		elem_table = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
		elem_rows = elem_table.find_elements_by_xpath("*")
	except Exception as ex:
		print(ex)
	
	print("Opening csv file..")
	#check if file already exists, if not write first row
	if not os.path.exists("coingecko_ex.csv"):
		try:
			with open("coingecko_ex.csv","w+", newline="") as csv_file:
				writer = csv.writer(csv_file)
				writer.writerow(["Position","Exchange Name","Type","Trust Score","24h volume (normalized)","24h volume","Website visits","# Coins", "# Pairs"])
		except Exception as ex:
			print("Error in opening the csv file")
	print("csv file opened successfully")
	#open new csv file
	try:
		with open("coingecko_ex.csv","a", newline="") as csv_file:
			writer = csv.writer(csv_file)
			#each row from the Coingecko table is splitted into tokens 
			print("Writing rows..")
			for row in elem_rows:
				features_list = []
				cleaned_row = row.text.replace("PHYSICAL","")
				cleaned_row = cleaned_row.replace("TRADING INCENTIVES","")
				features_list = cleaned_row.split()
				#if third field is not CEX or DEX then must be second name of exchange that will be added to the name
				if ("Centralized" not in features_list[2]) and ("Decentralized" not in features_list[2]):
					second_name_ex = features_list[2]
					features_list.pop(2)
					features_list[1] = features_list[1]+"-"+second_name_ex
				writer.writerow(features_list)
	except Exception as ex:
		print("Something went wrong while writing data\n"+ex)
	print("Writing was successful!")


print("Establishing connection to Coingecko website..")
try:
	driver.get("https://www.coingecko.com/en/exchanges") #navigate on the home page (exchanges section)
except Exception as ex:
	print("Could not connect to the Coingecko website, check your connection or the website status")
print("Connection successful!")


#last page to be accessed from web page
last_page = 3
for i in range(2,last_page+1): #
	print_ex_features()
	
	print("Accessing page #"+str(i)+"..");
	#access next page
	elem_next_page = driver.find_element_by_xpath('//a[@href="/en/exchanges?page='+str(i)+'"]')
	elem_next_page.click()

#print features for last page
print_ex_features()

driver.close()



