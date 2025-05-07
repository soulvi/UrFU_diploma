import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

# Функция для повторных попыток подключения
def fetch_with_retries(url, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response
            else:
                print(f"Попытка {attempt + 1}: Ошибка при запросе {url} - статус код {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Попытка {attempt + 1}: Ошибка при запросе {url} - {e}")
        time.sleep(delay)
    return None

# Функция для извлечения данных с страницы курса
def get_course_details(course_url):
    response = fetch_with_retries(course_url)
    if response:
        soup = BeautifulSoup(response.text, "html.parser")

        # Извлекаем описание курса
        description_element = soup.find("div", class_="description")
        description = description_element.get_text(strip=True) if description_element else "Нет данных"

        # Извлекаем раздел "О курсе"
        about_course_element = soup.find("h2", id="about")
        about_course = about_course_element.find_next("div", class_="catalog-block-content").get_text(
            strip=True) if about_course_element else "Нет данных"

        # Извлекаем раздел "Формат"
        course_format_element = soup.find("h2", id="course_format")
        course_format = course_format_element.find_next("div", class_="catalog-block-content").get_text(
            strip=True) if course_format_element else "Нет данных"

        # Извлекаем раздел "Программа курса"
        syllabus_element = soup.find("h2", id="syllabus")
        syllabus = syllabus_element.find_next("div", class_="catalog-block-content").get_text(
            strip=True) if syllabus_element else "Нет данных"

        # Извлекаем раздел "Результаты обучения"
        result_knowledge_element = soup.find("h2", id="result_knowledge")
        result_knowledge = result_knowledge_element.find_next("div", class_="catalog-block-content").get_text(
            strip=True) if result_knowledge_element else "Нет данных"

        # Извлекаем раздел "Формируемые компетенции"
        competence_element = soup.find("h2", id="competence")
        competence = competence_element.find_next("div", class_="catalog-block-content").get_text(
            strip=True) if competence_element else "Нет данных"

        # Извлекаем раздел "Направления подготовки"
        groups_element = soup.find("h2", id="groups")
        groups = groups_element.find_next("div", class_="catalog-block-content").get_text(
            strip=True) if groups_element else "Нет данных"

        return {
            "description": description,
            "about_course": about_course,
            "course_format": course_format,
            "syllabus": syllabus,
            "result_knowledge": result_knowledge,
            "competence": competence,
            "groups": groups
        }
    else:
        print(f"Не удалось подключиться к {course_url} после нескольких попыток")
        return None

# Функция для обработки одной страницы
def process_page(page_number, unique_links):
    url = f"https://openedu.ru/catalog/searchjs?=undefined&type=course&page={page_number}&size=15"
    response = fetch_with_retries(url)
    if response:
        data = response.json()
        courses = []
        for item in data.get("data", []):
            title = item.get("title")
            relative_link = f"/course/{item['uni_slug']}/{item['entity_slug']}/?session={item['session_slug']}"
            full_link = "https://openedu.ru" + relative_link

            # Проверяем, есть ли уже такая ссылка в множестве уникальных ссылок
            if full_link not in unique_links:
                unique_links.add(full_link)

                # Извлекаем данные с страницы курса
                course_details = get_course_details(full_link)
                if course_details:
                    course_info = {
                        "title": title,
                        "link": full_link,
                        **course_details
                    }
                    courses.append(course_info)

        print(f"Страница {page_number} успешно обработана. Найдено {len(courses)} записей.")
        return courses
    else:
        print(f"Не удалось подключиться к странице {page_number} после нескольких попыток")
        return []

def main():
    total_pages = 78
    all_courses = []
    unique_links = set()

    # ThreadPoolExecutor для многопоточной обработки
    with ThreadPoolExecutor(max_workers=10) as executor: 
        # Создаем задачи для каждой страницы
        futures = [executor.submit(process_page, page_number, unique_links) for page_number in range(1, total_pages + 1)]

        # Обрабатываем результаты по мере их завершения
        for future in as_completed(futures):
            try:
                courses = future.result()
                all_courses.extend(courses)
            except Exception as e:
                print(f"Ошибка при обработке страницы: {e}")

    # Сохраняем все данные в файл
    with open("all_courses.json", "w", encoding="utf-8") as f:
        json.dump(all_courses, f, ensure_ascii=False, indent=4)

    print(f"Всего собрано {len(all_courses)} уникальных записей. Данные сохранены в all_courses.json.")


if __name__ == "__main__":
    main()
