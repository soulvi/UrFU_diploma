import time
import random
import logging
import requests
import psycopg2
import re
import schedule
from datetime import datetime
from bs4 import BeautifulSoup

# Данные для авторизации в API HeadHunter
CLIENT_ID = ""
CLIENT_SECRET = ""
TOKEN_URL = "https://hh.ru/oauth/token"

# Конфигурация базы данных
DB_CONFIG = {
    'dbname': 'default_db',
    'user': 'gen_user',
    'password': 'a@X8bD32xFcB',
    'host': '94.241.171.32',
    'port': '5432'
}

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Получение токена доступа
def get_access_token():
    logging.info("Получение токена доступа...")
    try:
        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
        )
        response.raise_for_status()
        logging.info("Токен доступа успешно получен.")
        return response.json()["access_token"]
    except Exception as e:
        logging.error(f"Ошибка при получении токена: {e}")
        return None

# Создание таблицы vacancies
def create_table(conn):
    logging.info("Создание таблицы 'vacancies'...")
    cursor = conn.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            company VARCHAR(200),
            industry VARCHAR(300),
            title VARCHAR(200),
            keywords TEXT,
            skills TEXT,
            experience TEXT,
            salary VARCHAR(50),
            url VARCHAR(200) UNIQUE,  -- Уникальный URL для каждой вакансии
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            responsibility TEXT,
            requirement TEXT
        )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    logging.info("Таблица 'vacancies' успешно создана.")

# Расширенные шаблоны для извлечения данных
RESPONSIBILITY_PATTERNS = [
    r'(?:Задачи|Функционал|Миссия|Обязанности|Основные задачи)[:\-]?\s*(.*?)(?=(?:Требования|Условия|Что мы предлагаем|$))',
    r'(?:Вам предстоит|Тебе предстоит|Ваши задачи будут включать)[:\-]?\s*(.*?)(?=(?:Требования|$))',
    r'(?:Чем вам предстоит заниматься|Что предстоит делать)[:\-]?\s*(.*?)(?=(?:Мы ожидаем|$))'
]

REQUIREMENT_PATTERNS = [
    r'(?:Требования|Мы ожидаем|Эта вакансия для вас, если)[:\-]?\s*(.*?)(?=(?:Условия|Что мы предлагаем|$))',
    r'(?:Что важно для нас|Наши ожидания от кандидатов)[:\-]?\s*(.*?)(?=(?:Компенсация|$))',
    r'(?:Вы нам подходите если|Нам интересен опыт)[:\-]?\s*(.*?)(?=(?:График работы|$))'
]

OFFER_PATTERNS = [
    r'Условия[:\-]?\s*',
    r'Что мы предлагаем[:\-]?\s*',
    r'Компенсация[:\-]?\s*',
    r'График работы[:\-]?\s*'
]

def extract_section(text, patterns):
    """Извлекает секцию по шаблонам"""
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    return None

def clean_section(text):
    """Очищает извлеченный текст"""
    if not text:
        return None
    # Удаляем маркеры типа •, -, *
    text = re.sub(r'[\•\-\*\]', '', text)
    # Удаляем лишние переносы строк
    text = re.sub(r'\n{2,}', '\n', text)
    return text.strip()

# Функция для получения вакансий
def get_vacancies(city, vacancy, page, access_token):
    logging.info(f"Получение вакансий для '{vacancy}' в городе '{city}' (страница {page})...")
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': f"{vacancy} {city}",
        'area': city,
        'per_page': 100,
        'page': page
    }
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    logging.info(f"Вакансии для '{vacancy}' в городе '{city}' (страница {page}) успешно получены.")
    return response.json()

# Получение навыков вакансии
def get_vacancy_skills(vacancy_id, access_token):
    logging.info(f"Получение навыков для вакансии ID {vacancy_id}...")
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    skills = [skill['name'] for skill in data.get('key_skills', [])]
    logging.info(f"Навыки для вакансии ID {vacancy_id} успешно получены.")
    return ', '.join(skills)

# Получение отрасли компании
def get_industry(company_id):
    logging.info(f"Получение отрасли для компании ID {company_id}...")
    if company_id is None:
        return 'Unknown'

    url = f'https://api.hh.ru/employers/{company_id}'
    response = requests.get(url)
    if response.status_code == 404:
        return 'Unknown'
    response.raise_for_status()
    data = response.json()

    if 'industries' in data and len(data['industries']) > 0:
        logging.info(f"Отрасль для компании ID {company_id} успешно получена.")
        return data['industries'][0].get('name')
    return 'Unknown'

def clean_text(text):
    """Удаляет <highlighttext> из текста."""
    return re.sub(r"</?highlighttext>", "", text)

def clean_html(html_text):
    """Удаляет HTML-теги и возвращает чистый текст."""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text()

def get_vacancy_details(vacancy_id, access_token):
    """Получает навыки и полное описание вакансии."""
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Навыки
    skills = [skill['name'] for skill in data.get('key_skills', [])]
    skills_text = ', '.join(skills)

    # Полное описание
    description = data.get('description', '')

    # Извлекаем секции
    responsibility = None
    requirement = None

    if description:
        # Ищем обязанности
        responsibility = extract_section(description, RESPONSIBILITY_PATTERNS)
        if responsibility:
            responsibility = clean_section(responsibility)

        # Ищем требования
        requirement = extract_section(description, REQUIREMENT_PATTERNS)
        if requirement:
            requirement = clean_section(requirement)

    return skills_text, description, responsibility, requirement

# Функция для парсинга вакансий
def parse_vacancies(access_token):
    logging.info("Начало парсинга вакансий...")
    cities = {
        'Екатеринбург': 1
    }

    vacancies = [
         'Аналитик данных', 'Аналитик BI', 'Продуктолог',
 'Продуктовый аналитик', 'Дата аналитик','Аналитик-исследователь',
 'ML специалист','ML OPS инженер', 'ML-разработчик', 'Data Science',
 'Data Engineer', 'Data Analyst', 'Data Engineer', 'Data Science', 'Data Scientist',
 'Product Analyst','Machine Learning', 'ML Engineer',
 'Machine Learning Engineer','Computer vision', 'Big Data'
    ]

    with psycopg2.connect(**DB_CONFIG) as conn:
        create_table(conn)

        for city, city_id in cities.items():
            for vacancy in vacancies:
                page = 0
                while True:
                    try:
                        data = get_vacancies(city_id, vacancy, page, access_token)

                        if not data.get('items'):
                            logging.info(f"Вакансии для '{vacancy}' в городе '{city}' закончились.")
                            break

                        with conn.cursor() as cursor:
                            for item in data['items']:
                                if vacancy.lower() not in item['name'].lower():
                                    continue  # Пропустить, если название вакансии не совпадает

                                # Получение навыков и полного описания
                                skills, description, responsibility, requirement = get_vacancy_details(item['id'], access_token)

                                # Очистка текстовых полей
                                title = clean_text(f"{item['name']} ({city})")
                                keywords = clean_html(description)  # Очистка HTML-тегов из описания
                                cleaned_skills = clean_text(skills)

                                # Остальные поля
                                company = item['employer']['name']
                                industry = get_industry(item['employer'].get('id'))
                                experience = item['experience'].get('name', '')
                                salary = item['salary']
                                if salary is None:
                                    salary = "з/п не указана"
                                else:
                                    salary = salary.get('from', '')
                                url = item['alternate_url']

                                # Проверка, существует ли уже такая запись
                                cursor.execute("SELECT id FROM vacancies WHERE url = %s", (url,))
                                if cursor.fetchone() is None:
                                    # Вставка данных, если запись не существует
                                    insert_query = """
                                        INSERT INTO vacancies 
                                        (city, company, industry, title, keywords, skills, experience, salary, url, responsibility, requirement) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """
                                    cursor.execute(insert_query,
                                                   (city, company, industry, title, keywords, cleaned_skills, experience,
                                                    salary, url, responsibility, requirement))
                                    logging.info(f"Добавлена новая вакансия: {title}")
                                else:
                                    logging.info(f"Вакансия уже существует: {title}")

                            if page >= data['pages'] - 1:
                                break

                            page += 1
                            time.sleep(random.uniform(3, 6))  # Задержка между запросами

                    except requests.HTTPError as e:
                        logging.error(f"Ошибка при обработке города {city}: {e}")
                        continue

        conn.commit()

    logging.info("Парсинг завершен. Данные сохранены в базе данных PostgreSQL.")

# Основная функция для запуска парсинга
def main():
    logging.info("Запуск парсинга...")

    access_token = get_access_token()
    if not access_token:
        logging.error("Не удалось получить токен доступа. Парсинг отменен.")
        return

    try:
        parse_vacancies(access_token)
    except Exception as e:
        logging.error(f"Ошибка при выполнении задачи парсинга: {e}")

# Если скрипт запускается напрямую, вызываем main()
if __name__ == "__main__":
    main()
