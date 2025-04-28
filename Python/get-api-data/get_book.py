# 导入依赖包
import requests
from bs4 import BeautifulSoup

# 请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
}
payload = ''
url = 'https://www.szhlsf.com/booktxt/39000997116/'


# 获取数据
def get_data():
    books_url = 'https://www.szhlsf.com'
    # 获取请求
    response = requests.request("GET", url, headers=headers, data=payload)
    # 设置字符编码格式
    response.encoding = 'gbk'
    # 使用BeautifulSoup以lxml解析器的方式将数据进行解析
    soup = BeautifulSoup(response.text, 'lxml')
    # 提取网页中有class为book-chapter-list的div里面的内容
    div_result = soup.find('div', class_='book-chapter-list')
    # 进一步提取ul组件中的内容
    ul_result = div_result.find_all('ul', class_='cf')[1]
    # 进一步获取超链接
    a_result = ul_result.find_all('a')

    # 打开文件，并使用utf-8编码格式
    with open('./data/book/changyeyuhuo.html', 'w', encoding='utf-8') as file:
        # 写入HTML文件开始标签和头部信息
        file.write('<html>\n')
        file.write('<head>\n')
        file.write('<title>长夜余火火</title>\n')
        file.write('</head>\n')
        file.write('<body>\n')
        # 遍历目录及链接
        for i in a_result:
            # 获取章节标题
            chapter_title = i.text
            # 获取章节检测
            print(chapter_title)
            # 生成包装标题的HTML
            title_html = f'<h2>{chapter_title}</h2><br/>'
            # 将标题HTML写入HTML文件中
            file.write(title_html)
            # 获取书本链接
            book_url = books_url + str(i.get('href'))
            response1 = requests.request("GET", book_url, headers=headers, data=payload)
            # 设置字符编码格式
            response1.encoding = 'gbk'
            # 使用BeautifulSoup以lxml解析器的方式将数据进行解析
            soup1 = BeautifulSoup(response1.text, 'lxml')
            # 提取div
            div_result1 = soup1.find('div', class_='txt')
            # 获取替换后的内容并移除 HTML 标签
            result_text = div_result1.get_text()
            # 生成包装内容的HTML
            content_html = f'<p>{result_text}</p><br/>'
            # 将内容HTML写入HTML文件中
            file.write(content_html)
        # 写入HTML文件结束标签
        file.write('</body>\n')
        file.write('</html>\n')


def main():
    get_data()


if __name__ == '__main__':
    main()