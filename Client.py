import zmq
import requests
from bs4 import BeautifulSoup
import json
import time,datetime
import threading


def get_products_urls_for_all_category(index_url):
    """
    get the list of all product by category from web site index
    :param index_url:
    :return: product_category_url_list
    """
    # Step 1: Sending a HTTP request to a index_url
    try:
        reqs = requests.get(index_url)
    except Exception as Ex:
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text": str(Ex), "url": index_url, "methode": "get_products_urls_for_all_category"}, outfile)
            outfile.write("\n")
        pass
    # Step 2: Parse the html content
    soup = BeautifulSoup(reqs.text, 'lxml')

    # Step 3: init empty list to store all products category urls
    product_category_url_list = []
    try:
        # Step 4: Analyze the HTML tag to extract products category urls
        for tag in soup.find_all('div', {'id': "onglet_nav_1"}):
            products = tag.find_all("ul")[0]
            lis = products.find_all("li")
            for li in lis:
                print(li.a.get('href'))
                product_category_url_list.append(li.a.get('href'))
    except Exception as ex:
        print('report url execption')
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text":str(ex),"url":index_url,"methode":"get_products_urls_for_all_category"}, outfile)
            outfile.write("\n")
        pass
    return product_category_url_list


def get_urls_list_for_each_category(categ_url):
    """
    get list of all product en a given category
    :param categ_url:
    :return: all_products_urls_by_category
    """
    # Step 1: Sending a HTTP request to a categ_url
    try:
        reqs = requests.get(categ_url)
    except Exception as Ex:
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text": str(Ex), "url": categ_url, "methode": "get_urls_list_for_each_category"}, outfile)
            outfile.write("\n")
            pass
    # Step 2: Parse the html content
    soup = BeautifulSoup(reqs.text, 'lxml')

    # Step 3: init empty list to store all products urls by category
    all_products_urls_by_category = []
    try:
        # Step 4: Analyze the HTML tag to extract urls for given  category
        for tag in soup.find_all('div', {'id': "boutiqueV2"}):
            products = tag.find_all("ul")[0]
            lis = products.find_all("li")
            for li in lis:
                all_products_urls_by_category.append(li.a.get('href'))
    except Exception as ex:
        print('report url execption')
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text":str(ex),"url":categ_url,"methode":"get_urls_list_for_each_category"}, outfile)
            outfile.write("\n")
        pass
    return all_products_urls_by_category


def get_final_url_product(s_url):
    """
    extract final product url in which we can get the product info
    from list of all products web page
    :return:
    """
    # Step 1: Sending a HTTP request to a index_url
    try:
        reqs = requests.get(s_url)
    except Exception as Ex:
        print('report url execption')
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text": str(Ex), "url": s_url, "methode": "get_urls_list_for_each_category"}, outfile)
            outfile.write("\n")
            pass
    # Step 2: Parse the html content
    soup = BeautifulSoup(reqs.text, 'lxml')
    final_product_urls_list = []

    try:
        # Step 3: Analyze the HTML tag to extract final url form list of all products
        for tag in soup.find_all('div', attrs={'class': "col-xs-6"}):
            for i in tag.find_all("a"):
                if i.get('href') and "https://" in i.get('href'):
                    print(i.get('href'))
                    final_product_urls_list.append(i.get('href'))
    except Exception as ex:
        print('report url execption')
        with open('error_log_file.json', 'a') as outfile:
            json.dump({"exception_text":str(ex),"url":s_url,"methode":"get_final_url_product"}, outfile)
            outfile.write("\n")
        pass
    #to make sure that there is no dublications
    return list(dict.fromkeys(final_product_urls_list))
def get_products_list_urls_form_index(index_url):
    """
    get list of all products url after scraping index web page and get all products category urls
    then for each category get the url of every product and finally get the product info
    :param index_url: web site index url houra.fr
    :return: products_url_list
    """
    start_time = time.time()
    product_category_url_list = get_products_urls_for_all_category(index_url)
    all_products_url = []
    final_urls_list = []
    for url in product_category_url_list[0:2]:
        all_products_url += get_urls_list_for_each_category(url)
    for url_ in all_products_url:
        final_urls_list += get_final_url_product(url_)
    #save products url in txt file
    with open("product_urls.txt", "a") as products:
        [products.write(str(p)+"\n") for p in final_urls_list]
    report_info =dict(
    Start_Time =  str(start_time),
    End_Time = str(datetime.datetime.now()),
    Processing_Time_Seconds = str((time.time() - start_time)),
    Status = "Success",
    Methode="get_products_list_urls_form_index")
    with open("report_file.txt", "a") as reportfile:
        json.dump(report_info, reportfile)
        reportfile.write("\n")
    return final_urls_list



def therd_task_to_run_workers(li,port):
    """
    run multi process to share the Client task to all the workers
    :param li:  sub list of product urls
    :param port: worker port
    :return: product infos
    """
    for url in li:
        print(f"Connecting to worker…{port}")
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://localhost:{port}")

        print("Sending request %s …" % url)
        socket.send_string(url)

        #  Get the reply.
        message = json.loads(socket.recv())
        print("Received reply  [ %s ]" % (message))
        with open('output.json','a') as outfile:
            if message:
                data={}
                data[str(message["Product"])]=message
                json.dump(data,outfile)
                outfile.write("\n")

    return "extracting product info successfully Done!"


def create_and_execute_new_worker(prots):
    """
    create new workers by creating worker.py copy and executing the new workers withh the given ports

    :param prots: list of ports
    :return: create new workers file and execute them in bg
    """
    from shutil import copyfile
    import subprocess ,sys
    pids=[]
    for p in prots:
        copyfile("./Worker.py", f"./Worker{p}.py")
        process = subprocess.Popen([sys.executable, f"./Worker{p}.py" ,f'{p}'],close_fds=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        pids.append(process.pid)
    print(pids)
    return pids


def get_product_info_from_multiclient(ports):
    """
    get list of url from products_url.txt and run parallel workers to extract product info
    :return:
    """
    #read urls from file created py the first project version to increase processing time
    with open("./product_urls.txt", "r") as f:
        product_list_urls = f.read().splitlines()
    #get some info about the execution process execution time
    start_time = time.time()
    st = str(datetime.datetime.now())

    pids=create_and_execute_new_worker(ports)
    #split list of urls to sublist by the number of the workers
    sublists = [product_list_urls[i:i +  len(product_list_urls)//len(ports)] for i in range(0, len(product_list_urls),  len(product_list_urls)//len(ports))]
    for i in ports:
        threads = []
        threads.append(threading.Thread(target=therd_task_to_run_workers, args=(sublists[ports.index(i)],i)))
        # starting thread
        threads[-1].start()
    for t in threads:
        t.join()
    report_info =dict(
    Start_Time =  st,
    End_Time = str(datetime.datetime.now()),
    Processing_Time_Seconds = str((time.time() - start_time)),
    Status = "Success",
    Methode="get_product_info_from_client")
    with open("report_file.txt", "a") as reportfile:
        json.dump(report_info, reportfile)
        reportfile.write("\n")
    print("Done!")
    #clean up delete created workers file and stop runing scripts process
    import os
    for p in pids:
        os.system(f"kill -9 {p}")
    for pr in ports:
        print(pr)
        os.system(f"rm  ./Worker{pr}.py")

    return "Scraping Completed successfully !"

#add or delete port to get more workers
ports=[5561,5562,5563,5564]

index_url="https://www.houra.fr/"
get_product_info_from_multiclient(ports)