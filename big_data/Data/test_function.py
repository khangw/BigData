import requests
from bs4 import BeautifulSoup

convertName = {
    "Năm phát hành": "releaseYear",
    "Quốc gia": "country",
    "Thể loại": "genre",
    "Đạo diễn": "director",
    "Thời lượng": "duration",
    "Diễn viên": "actor"
}

link_film = 'https://phimmoichill.biz/info/ma-sieu-quay-pm16192'
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(link_film, headers=headers)
print(response.status_code)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
    body = soup.find('ul', class_='block-film')
    data = {}

    # Tìm tất cả các thẻ <li> trong ul
    for li in soup.find_all('li'):
        # Tìm thẻ <label> trong mỗi <li>
        label = li.find('label')
        if label:
            # Lấy tên thuộc tính là văn bản của <label>
            label_name = label.text.strip().replace(":", "")

            # Lấy giá trị sau <label>, có thể là text hoặc trong thẻ <span>, <a>, v.v.
            value = None
            # Tìm các thẻ con như <span>, <a> trong mỗi <li>
            if li.find('span'):
                value = li.find('span').text.strip()
            elif li.find('a'):
                value = ', '.join([a.text for a in li.find_all('a')])
            elif li.find(string=True):
                value = li.findAll(string=True)[-1]

            # Lưu cặp key-value vào từ điển
            if value:
                data[label_name] = value
    print(data)
else:
    print("Failed to retrieve the page.")
