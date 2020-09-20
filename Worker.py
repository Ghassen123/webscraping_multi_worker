import requests
from bs4 import BeautifulSoup
import zmq
import json,sys
#Get prot as fist arg
argv = sys.argv[1]
print(argv)
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind(f"tcp://*:{argv}")

def get_product_info_from_url(url):
    """
    get product info from a given  url
    :param url: product url
    :return: product info json dict
    """
    try:
        # Step 1: Sending a HTTP request to a index_url
        reqs = requests.get(url)
    except Exception as Ex:
        print('report url execption')
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text": str(Ex), "url": str(url), "methode": "get_product_info_from_url"}, outfile)
            outfile.write("\n")
        return {}
    # Step 2: Parse the html content
    soup = BeautifulSoup(reqs.text, 'lxml')
    product_info = {}
    try:
        # Step 4: Analyze the HTML tag to extract product infos

        for tag in soup.find_all('div', attrs={'class': "row no-padding FicheArticleRspv"}):
            title=tag.find('div', attrs={'class': "col-sm-12"}).find('h1').text
            t= tag.find('div', attrs={'class': "row no-padding fa_infos"}).find('div', attrs={'class': "col-sm-7 description-produit"})
            marque=t.find('div', attrs={'class': "art_marque"}).text
            r = tag.find('div', attrs={'class': "row no-padding fa_infos"}).find('div', attrs={'class': "col-sm-5"}).find('div', attrs={'class': "promobri bri_rj"})
            promo =  r.text if r else "N/A"
            info = tag.find('div', attrs={'class': "row no-padding fa_infos"}).find('div', attrs={'class': "col-sm-7 description-produit"}).text
            info = " ".join(info.split("\n"))
            price = tag.find('div', attrs={'class': "row no-padding fa_infos"}).find('div', attrs={'class': "col-sm-5"}).find('div', attrs={'class': "art_prix"}).text
            volume_price = tag.find('div', attrs={'class': "row no-padding fa_infos"}).find('div', attrs={'class': "col-sm-5"}).find('div', attrs={'class': "art_prix_volume"}).text
            product_info = dict(Product=marque,Title=title,product_info=info,promo=promo,price=price,volume_price=volume_price)
    except Exception as ex:
        print('report url execption')
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text": str(ex), "url": str(url), "methode": "get_product_info_from_url"}, outfile)
            outfile.write("\n")
        pass
    return product_info
while True:
    # message = json.loads(socket.recv())
    message = socket.recv()
    print("Received request: %s" % message)
    myjson = json.dumps(get_product_info_from_url(message))
    #  Send reply back to client
    socket.send_string(myjson)


