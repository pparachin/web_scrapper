import concurrent
import requests
import unicodedata
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 5


def main():
    url_category = "https://www.alza.cz/mobily/18843445.htm"
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/108.0.0.0 Safari/537.36',
        'referer': 'https://google.cz',
        'Cache-Control': 'no-cache',
        'Accept-Language': 'en-US,en;q=0.9'
    }

    pages_count = find_count_of_pages(url_category, headers)
    urls = get_all_urls_cat(url_category, headers, int(pages_count[0]))
    products = find_all_products(urls, headers)
    save_products_to_database(products, pages_count[1])


def get_all_urls_cat(url, headers, count_of_pages):
    urls = []
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    for link in soup.select(".browsinglink"):
        urls.append(link.get('href'))
    for i in range(2, count_of_pages+1):
        page = requests.get(f"{url[:-4]}-p{i}.htm", headers=headers)
        soup = BeautifulSoup(page.content, "html.parser")
        for link in soup.select(".browsinglink"):
            urls.append(link.get('href'))

    del urls[1::2]
    print(f"Bylo nalezeno {len(urls)} url adres produktů")
    return urls


def find_count_of_pages(url, headers):
    new_url = url.removesuffix('.htm')
    page = requests.get(new_url+"-p4.htm", headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    print(soup)
    div_pages = soup.find("div", {"id": "pagerbottom"})
    count = div_pages.select("span")[7].text
    div_pages = soup.find("a", {"class": "last"}).text
    print("Počet stránek získán")
    return [count, div_pages]


def download_product_data(url, headers):
    product = {}
    try:
        page = requests.get(f"https://www.alza.cz{url}", headers=headers)
        soup = BeautifulSoup(page.content, "html.parser")

        product_name = soup.select("h1")[0].text
        product_name = product_name.replace('\n', '')
        product["name"] = product_name

        price = soup.select(".price-box__price")[0].text
        price = unicodedata.normalize("NFKD", price).replace(',', '').replace('-', '').replace(" ", '')
        product["price"] = price

        rating = soup.select(".ratingValue")[0].text
        rating = float(rating.replace(',', '.'))
        product["rating"] = rating

        ipc = soup.find("span", class_="moreInfo")
        ipc = ipc.select("span")[1].select("span")[1].text
        product["ipc"] = ipc

        mpn = soup.find("span", class_="moreInfo")
        mpn = mpn.select("span")[4].select("span")[1].text
        product["mpn"] = mpn

        product["url"] = f"https://www.alza.cz{url}"

    except Exception:
        print(f"Chyba při zpracování produktu {url}")
        return None

    # Kontrola zda slovník obsahuje všechny hodnoty
    required_fields = ["name", "price", "rating", "ipc", "mpn", "url"]
    if not all(field in product for field in required_fields):
        return None

    return product


def find_all_products(urls, headers):
    products = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_url = {executor.submit(download_product_data, url, headers): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                product = future.result()
                if product:
                    products.append(product)
            except Exception as exc:
                print(f'Chyba při scrapovaní URL {url}: {exc}')

    print(f'Nalezeno {len(products)} produktů')
    return products


def save_products_to_database(products, category_name):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="foxily_data_mining"
        )

        cursor = connection.cursor()

        for product in products:

            now = datetime.now()
            query = "INSERT INTO articles (name, price, url, category_name, rating, internal_product_code, mpn, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (product["name"], product["price"], product["url"], category_name, product["rating"], product["ipc"], product["mpn"], now, now))

        connection.commit()

        cursor.close()
        connection.close()

        print("Produkty byli úspešně uloženy do databáze!")

    except Error as error:
        print(f"Chyba při ukládaní produktu do databáze: {error}")


if __name__ == "__main__":
    main()

