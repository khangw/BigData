import argparse
import json
from typing import List
import requests
from bs4 import BeautifulSoup


def setup_file(filename, is_append):
    mode = "a+" if is_append else "w"
    bracket = ']' if is_append else '['

    with open(filename, mode, encoding='utf-8') as f:
        f.writelines(bracket)


def write_file(filename, data, delimiter):
    with open(filename, "a+", encoding="utf-8") as f:
        f.writelines(delimiter)
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_list_link(start_page: int, end_page: int) -> List[str]:
    return [f"https://phimmoichill.biz/list/phim-le/page-{page}/" for page in range(start_page, end_page + 1)]


def get_header_film_list(link_by_pages: List[str]) -> List[dict]:
    header_film_list = []

    for link in link_by_pages:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(link, headers=headers)
        print(f"Link: {link} Status Code: {response.status_code}")

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            body = soup.find_all('li', class_='item')

            for ele in body:
                header_film = {}
                link_film = ele.find('a')['href']
                name = ele.find('h3').text.strip() if ele.find('h3') else "No Title"
                header_film['name'] = name
                header_film['link'] = link_film
                header_film_list.append(header_film)
        else:
            print(f"Failed to retrieve the page {link}.")

    return header_film_list


def get_full_data_films(film_list: List[dict]) -> List[dict]:
    full_film_data = []

    for film in film_list:
        film_link = film['link']
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(film_link, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            film_data = {}

            for li in soup.find_all('li'):
                label = li.find('label')
                if label:
                    label_name = label.text.strip().replace(":", "")
                    value = None

                    if li.find('span'):
                        value = li.find('span').text.strip()
                    elif li.find('a'):
                        value = ', '.join([a.text for a in li.find_all('a')])
                    elif li.find(string=True):
                        value = li.findAll(string=True)[-1]

                    if value:
                        film_data[label_name] = value

            content = soup.find('div', class_='film-content')
            if content:
                strong_tag = content.find('strong')
                if strong_tag:
                    content_film = strong_tag.text.strip()
                    film_data['content'] = content_film

            full_film_data.append({**film, **film_data})
        else:
            print(f"Failed to retrieve the page for {film_link}.")

    return full_film_data


def crawl_data(filename, data_film_list):
    print(f"Writing {filename} ...")
    setup_file(filename, False)
    delimiter = ""

    for data_film in data_film_list:
        print(f"Writing film: {data_film['name']}")
        write_file(filename, data_film, delimiter)
        delimiter = ",\n"  # Add a comma between each film data

    setup_file(filename, True)  # Ensure the file ends with a closing bracket


if __name__ == "__main__":
    print("Parsing Args")
    parser = argparse.ArgumentParser()
    parser.add_argument("startPage", type=int)
    parser.add_argument("endPage", type=int)
    args = parser.parse_args()
    print(f"Start crawling from page {args.startPage} to {args.endPage}")

    links = get_list_link(args.startPage, args.endPage)
    header_film_list = get_header_film_list(links)
    data_film_list = get_full_data_films(header_film_list)

    file_name = f"data_film_{args.startPage}_{args.endPage}.json"
    crawl_data(file_name, data_film_list)
