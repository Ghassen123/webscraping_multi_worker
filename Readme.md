# Distributed Scraping System 
#Mulit Workers 
Scraping houra.fr/ web site to get products information.

# Client:
   old version
  > Client will scrap the index url to get the list of all products
  > Scrap urls of all product categories from the index url https://www.houra.fr/
  > Scrap list of urls of all products in given category for exemple https://www.houra.fr/catalogue/le-marche-B3000059-1.html
  > Scrap list of product (https://www.houra.fr/catalogue/le-marche/fruits-et-legumes-B1487926-1.html for exemple) to get final product url in which we can find the product informations (fro exemple  https://www.houra.fr/patisson-blanc-bio-1-piece/1410315/) 
  > Save list of all product urls into  product_urls.json
>
new version
  - Read urls from file and create workers 
  - init ports list if you want to add or remove workers
  - Client will execute parallel jobs to share the ulrs to the workers 
# Workers
  - Workers will scrap the product url to get the produt informations
  - Save list of all product info into output.json
   

### Tech


* [Python] :
>-Requests
-ZeroMQ
-BeautifulSoup


### Installation

Install the dependencies and devDependencies and start the server.
after creating the virtuel env
PS: add or remove Ports list in Client.py if you want to decrease or increase workers
```sh
$ cd datagram_scraping_test_client
$ pip install -r req.txt
$ python3 Worker.py
$ python3 Client.py
```
# Reprots:
All Error or Exceptions will be stored into error_log_file.json 
Time spend in urls Scrapping and product informations extraction will be stored into report_file.txt 

