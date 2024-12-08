import argparse
import json
import re
from typing import List
import requests
from bs4 import BeautifulSoup


def setup_file(filename, is_append):
    mode = "a+" if is_append else "w"
    bracket = ']' if is_append else '['

    with open(filename, mode, encoding='utf-8') as f:
        f.write(bracket)  # Thay vì writelines, sử dụng write cho chuỗi nhỏ


def write_file(filename, data, delimiter):
    with open(filename, "a+", encoding="utf-8") as f:
        f.write(delimiter)  # Sử dụng write thay vì writelines
        json.dump(data, f, indent=2, ensure_ascii=False)



def get_list_link(start_page: int, end_page: int) -> List[str]:
    list_phim_bo = [f"https://motphim.ad/danh-sach/phim-bo?page={page}" for page in range(start_page, end_page + 1)]
    list_phim_le = [f"https://motphim.ad/danh-sach/phim-le?page={page}" for page in range(start_page, end_page//4+1)]
    return list_phim_bo + list_phim_le


def get_header_film_list(link_by_pages: List[str]) -> List[dict]:
    results = []

    for link in link_by_pages:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(link, headers=headers)
        # print(f"Link: {link} Status Code: {response.status_code}")
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            body = soup.find_all('h4', class_='title')

            for ele in body:
                header_film = {}
                link_film = ele.find('a')['href']
                name = ele.find('a').text.strip() if ele.find('a') else "No Title"
                header_film['name'] = name
                header_film['link'] = link_film
                header_film['type'] = 'Phim lẻ' if 'le' in link else 'Phim bộ'
                results.append(header_film)
        else:
            print(f"Failed to retrieve the page {link}.")

    return results


def get_full_data_films(film_list: List[dict]) -> List[dict]:
    full_film_data = []

    for film in film_list:
        film_link = film['link']
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(film_link, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            film_data = {}

            info = soup.find('div', class_='info-block')
            if info:
                h6_list = info.findAll('h6')
                for h6 in h6_list:
                    # Lấy label
                    label_name = h6.text.strip().replace("\n", "").split(":")[0].strip()

                    # Lấy value
                    value = ''
                    if h6.find('span'):  # Nếu có span bên trong
                        value = h6.find('span').text.strip()
                    elif h6.find('a'):  # Nếu có liên kết (a)
                        value = ', '.join([a.text.strip() for a in h6.find_all('a')])
                    else:
                        if '(' in h6.text.strip():
                            value = h6.text.strip().split('(')[-1].split(')')[0]  # Tách ra số năm nằm giữa dấu ngoặc
                            label_name = 'Năm phát hành'
                        else:
                            value = h6.text.strip().replace("\n", "").split(":", 1)[-1].strip()

                    value = re.sub(r'\s+', ' ', value)
                    # Lưu vào dictionary
                    if label_name and value:
                        film_data[label_name] = value

            movie_show_time = soup.find('div', class_='myui-player__notice')
            if movie_show_time:
                content_film = movie_show_time.text.strip().split(':')
                film_data['content'] = content_film[-1]

            div_star = soup.find('div', {'id': 'star'})
            if div_star:
                rate_score = div_star.get('data-score')
                film_data['Đánh giá'] = rate_score

            full_film_data.append({**film, **film_data})
        else:
            print(f"Failed to retrieve the page for {film_link}.")

    return full_film_data


def crawl_data(filename, data_film_lists):
    print(f"Writing {filename} ...")
    setup_file(filename, False)
    delimiter = ""

    for data_film in data_film_lists:
        # print(f"Writing film: {data_film['name']}")
        write_file(filename, data_film, delimiter)
        delimiter = ",\n"  # Add a comma between each film data

    setup_file(filename, True)  # Ensure the file ends with a closing bracket


if __name__ == "__main__":
    print("Parsing Args")
    parser = argparse.ArgumentParser()
    parser.add_argument("startPage", type=int)
    parser.add_argument("endPage", type=int)
    args = parser.parse_args()
    print(f"Start crawling from page {args.startPage} to {args.endPage} ...")

    links = get_list_link(args.startPage, args.endPage)
    print(links)
    header_film_list = get_header_film_list(links)
    data_film_list = get_full_data_films(header_film_list)

    file_name = f"data_film_{args.startPage}_{args.endPage}.json"
    crawl_data(file_name, data_film_list)
