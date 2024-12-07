import requests
import re
from bs4 import BeautifulSoup

convertName = {
    "Năm phát hành": "releaseYear",
    "Quốc gia": "country",
    "Thể loại": "genre",
    "Đạo diễn": "director",
    "Thời lượng": "duration",
    "Diễn viên": "actor"
}

link_film = 'https://motphim.ad/phim/vu-an-bo-ngo-ai-da-giet-jonbenet-ramsey'
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(link_film, headers=headers)
print(response.status_code)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
    film_data = {}

    info = soup.find('div', class_='info-block')
    if info:
        h6_list = info.findAll('h6')
        for h6 in h6_list:
            # Lấy label
            print(h6)
            label_name = h6.text.strip().replace("\n", "").split(":")[0].strip()

            # Lấy value
            value = None
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
        film_data['rate_score'] = rate_score

    print(film_data)
else:
    print(f"Failed to retrieve the page for {link_film}.")
