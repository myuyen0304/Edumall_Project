import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException,  TimeoutException, StaleElementReferenceException
import time
from tqdm import tqdm
import json
import pymongo

# Khởi tạo driver
def initialize_driver():
    driver = webdriver.Chrome()
    return driver

# Hàm khởi tạo MongoDB client
def initialize_mongodb():
    """Khởi tạo kết nối đến MongoDB."""
    client = pymongo.MongoClient('mongodb://localhost:27017')  # Chỉnh sửa URL này tùy thuộc vào môi trường MongoDB của bạn
    db = client['edumall']  # Tên database
    collection = db['edumall_data']  # Tên collection
    return collection

# Hàm thu thập liên kết khóa học từ các trang và ghi vào file
def get_course_links(driver, pages):
    """Lấy tất cả các liên kết khóa học từ các trang và lưu vào file."""
    with open('list_links_full2.txt', 'a') as file:
        for i in tqdm(range(1, pages + 1), desc="Processing Pages"):
            url = f"https://edumall.vn/vn/search?mode=&page={i}&size=12"
            driver.get(url)
            # Chờ cho đến khi các phần tử h3 xuất hiện trên trang với thời gian chờ 4 giây
            try:
                WebDriverWait(driver, 4).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//*[@class='styles_detail__ad7XV']/h3"))
                )
            except TimeoutException:
                print(f"Trang {i} không tải được các khóa học.")
                continue
            # Tìm và lấy các link khóa học
            elements = driver.find_elements(By.XPATH, "//*[@class='styles_detail__ad7XV']/h3")
            for element in elements:
                try:
                    link = element.get_attribute("href")
                    # Nếu chưa có 'https://edumall.vn' trong link, thêm vào
                    if 'https://edumall.vn' not in link:
                        link = 'https://edumall.vn' + link
                    file.write(link + '\n')
                except StaleElementReferenceException:
                    print(f"Phần tử trên trang {i} không còn tồn tại trong DOM, bỏ qua phần tử này.")
                    continue

# Hàm trích xuất dữ liệu khóa học từ các liên kết
def extract_course_data(driver, links):
    """Trích xuất dữ liệu khóa học từ các liên kết."""
    list_title = []
    list_author = []
    list_topic = []
    list_description = []
    list_what_you_will_learn = []
    list_price = []
    list_price_discount = []
    list_evaluate = []
    list_date = []
    list_chapter = []
    list_so_bai_hoc = []
    list_thoi_gian_hoc = []

    for link in tqdm(links, desc="Processing Links"):
        driver.get(link)
        time.sleep(3)
        
        try:
            title = driver.find_element(By.XPATH, '//*[@id="about"]/h1').text
        except NoSuchElementException:
            title = None
            
        try:
            author = driver.find_element(By.XPATH, '//*[@id="about"]/div[2]/div[1]/p[2]').text
        except NoSuchElementException:
            author = None
            
        try:
            topic = driver.find_element(By.XPATH, '//*[@id="client-main-layout"]/div[2]/div/div[1]/div/div/div/div[1]/nav/ol/li[3]/span/a').text
        except NoSuchElementException:
            topic = None

        try:
            description = driver.find_element(By.XPATH, '//*[@id="about"]/div[1]/div').text
        except NoSuchElementException:
            description = None
            
        # try:     
        #     elements = driver.find_elements(By.XPATH, '//div[contains(@class, "ant-space-item")]/p')
        #     learn = []
        #     if elements:
        #         for element in elements:
        #             learn.append(element.text)
        #     else:
        #         learn = None
        # except NoSuchElementException:
        #     learn = None

        try:
            # learn = driver.find_element(By.XPATH, '//div[contains(@class, "ant-space-item")]/p').text
            elements = driver.find_elements(By.XPATH, '//div[contains(@class, "ant-space-item")]/p')
            if elements:
                learn = [element.text for element in elements]
            else:
                learn = None
        except NoSuchElementException:
            learn = None
            
        try:
            price_element = driver.find_element(By.XPATH, '//div[contains(@class, "course-price_price__XwwKh")]/span[contains(@class, "line-through")]')
            price = price_element.text.strip()
            # Loại bỏ ký tự 'đ' và dấu phẩy
            price = ''.join(filter(str.isdigit, price))
        except NoSuchElementException:
            price = None

        try:                                                                        
            price_discount_element = driver.find_element(By.XPATH, '//div[contains(@class, "course-price_price__XwwKh")]/p')
            price_discount = price_discount_element.text.strip()
            # Loại bỏ ký tự 'đ' và dấu phẩy
            price_discount = ''.join(filter(str.isdigit, price_discount))
        except NoSuchElementException:
            price_discount = None

        try:
            evaluate = driver.find_element(By.XPATH, '(//*[@class="md:text-16 text-14 "])[1]').text
        except NoSuchElementException:
            evaluate = None

        try:
            date = driver.find_element(By.XPATH, '(//p[@class="md:text-16 text-14 font-medium"])[2]').text
        except NoSuchElementException:
            date = None

        try:
            chapter = driver.find_element(By.XPATH, '//*[@id="client-learning-path-detail-content"]/div/div/div[3]/div/div[1]/div/p[1]').text
        except NoSuchElementException:
            chapter = None

        try:
            so_bai_hoc = driver.find_element(By.XPATH, '//*[@id="client-learning-path-detail-content"]/div/div/div[3]/div/div[1]/div/p[3]').text
        except NoSuchElementException:
            so_bai_hoc = None

        try:
            thoi_gian_hoc = driver.find_element(By.XPATH, '//*[@id="client-learning-path-detail-content"]/div/div/div[3]/div/div[1]/div/p[5]').text
        except NoSuchElementException:
            thoi_gian_hoc = None

        list_title.append(title)
        list_author.append(author)
        list_topic.append(topic)
        list_description.append(description)
        list_what_you_will_learn.append(learn)
        list_price.append(price)
        list_price_discount.append(price_discount)
        list_evaluate.append(evaluate)
        list_date.append(date)
        list_chapter.append(chapter)
        list_so_bai_hoc.append(so_bai_hoc)
        list_thoi_gian_hoc.append(thoi_gian_hoc)

    return {
        'Coursename': list_title,
        'Author': list_author,
        'Topic': list_topic,
        'Describe': list_description,
        'What_you_will_learn': list_what_you_will_learn,
        'Oldfee': list_price,
        'Newfee': list_price_discount,
        'Rating': list_evaluate,
        'Last_updated': list_date,
        'Sections': list_chapter,
        'Lectures': list_so_bai_hoc,
        'Time': list_thoi_gian_hoc,
        'Link': links
    }

# def save_to_excel(data, filename='edumall_data.xlsx'):
#     """Lưu dữ liệu vào file Excel"""
#     df = pd.DataFrame(data)
#     df.to_excel(filename, index=False)
#     print(f"Dữ liệu đã được lưu vào file '{filename}'")

def clean_string(value):
    if isinstance(value, str):
        return ''.join(c for c in value if c.isprintable())
    return value

def save_to_excel(data, filename='edumall_data.xlsx'):
    """Lưu dữ liệu vào file Excel"""
    df = pd.DataFrame(data)
    # Làm sạch dữ liệu bằng map() cho từng cột
    df = df.apply(lambda col: col.map(clean_string))
    df.to_excel(filename, index=False)
    print(f"Dữ liệu đã được lưu vào file '{filename}'")
    
# Hàm Đọc dữ liệu vào file csv  
def save_to_csv(data, filename='edumall_data1.csv'):
    """Lưu dữ liệu vào file CSV"""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Dữ liệu đã được lưu vào file '{filename}'")
    
# def save_to_json(data, filename='edumall_data1.json'):
#     """Lưu dữ liệu vào file JSON"""
#     df = pd.DataFrame(data)
#     df.to_json(filename, force_ascii=False, orient='records', indent=4)
#     print(f"Dữ liệu đã được lưu vào file '{filename}'")

# Hàm Đọc dữ liệu vào file Json
def save_to_json(data, filename='edumall_data1.json'):
    """Lưu dữ liệu vào file JSON"""
    df = pd.DataFrame(data)
    with open(filename, 'a', encoding='utf-8') as file:
        json.dump(df.to_dict(orient='records'), file, ensure_ascii=False, indent=4)
    print(f"Dữ liệu đã được lưu vào file '{filename}'")
    
#Hàm lưu dữ liệu vào MongoDB
def save_to_mongodb(data, collection):
    """Lưu dữ liệu vào MongoDB."""
    df = pd.DataFrame(data)
    # Convert DataFrame to JSON, ensuring correct encoding
    json_data = df.to_json(orient='records', force_ascii=False)
    records = json.loads(json_data)
    collection.insert_many(records)
    print(f"Dữ liệu đã được lưu vào MongoDB")
    
# Hàm chính
def main():
    driver = initialize_driver()
    collection = initialize_mongodb()
    try:
        pages = 125  # Số lượng trang bạn muốn thu thập liên kết
        get_course_links(driver, pages)
        
        # Đọc lại các liên kết từ file để trích xuất dữ liệu
        with open('list_links_full2.txt', 'r', encoding='utf-8') as file:
            links = file.read().splitlines()
        
        data = extract_course_data(driver, links)
        save_to_csv(data)
        # save_to_excel(data)
        save_to_json(data)
        save_to_mongodb(data, collection)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
