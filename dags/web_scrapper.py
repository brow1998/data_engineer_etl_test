import requests
import csv
import json
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = 'https://www.americanas.com.br'
SEARCH_URL = BASE_URL + "/busca/notebook?conteudo={0}&filtro=%5B%7B%22id%22%3A%22loja%22%2C%22value%22%3A%22Americanas.com%22%2C%22fixed%22%3Afalse%7D%5D&ordenacao=relevance&origem=nanook&suggestion=true"

prices = []
urls = []

def get_search_items(soup): #
    for i in range(0, len(soup.find_all('script'))):
        try:
            if json.loads(soup.find_all('script')[i].text)['url'] != BASE_URL:
                urls.append(json.loads(soup.find_all('script')[i].text)['url'])
        except:
            pass    

def iterate_through_search_pages(search_link, offset=24): #
    aditional_string = '&limite=24&offset={0}'
    
    base_search = search_link + aditional_string
    first_page = base_search.format(24)

    while first_page:
        r = requests.get(first_page, timeout=5)
        soup = BeautifulSoup(r.content,'html.parser')
        offset += 24
        if soup.find_all('script')[7].text != '':
            get_search_items(soup) 
            first_page = base_search.format(offset)
        else:
            first_page = None

def get_product_data(soup):
    product = json.loads(soup.find_all('script')[6].text)
    id = product['url'].split('/')[4]

    return {'id': id,
            'sku': product.get('sku', None),
            'price': product['offers'].get('price', None),
            'description': product['name'],
            'url': product['url'],
            'url_pic': product.get('image', None),
           }

def collect_data(urls): 
    print(len(urls))
    for url in tqdm(urls):
        link = BASE_URL+url
        r = requests.get(link)
        soup = BeautifulSoup(r.content,'html.parser')    
        data = get_product_data(soup)
        prices.append(data)

def generate_csv(info, filename='webscrapper.csv'):
    with open(filename, 'w') as f:
        dic = csv.DictWriter(f, fieldnames=info[0].keys())
        dic.writeheader()
        dic.writerows(info)

def main():
    word_bag = ['notebook'] # Edit here

    for word in word_bag:
        print("Crawling word: {0}".format(word))
        
        search_link = SEARCH_URL.format(word)
        
        r = requests.get(search_link, timeout=5)
        soup = BeautifulSoup(r.content,'html.parser')
        get_search_items(soup) 
        iterate_through_search_pages(search_link)
    
    print("Collecting data")
    collect_data(urls)
    
    print("Generating CSV")
    generate_csv(prices)



if __name__ == '__main__':
    main()
    