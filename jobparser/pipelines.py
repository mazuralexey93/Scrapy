# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient


class JobparserPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_base = client.vacancies0408

    def process_item(self, item, spider):
        if spider.name == 'hhru':
            item['salary'] = self.process_salary_hh(item['salary'])
        else:
            item['salary'] = self.process_salary_sj(item['salary'])

        collection = self.mongo_base[spider.name]
        collection.insert_one(item)

        return item

    def process_salary_hh(self, salary):
        salary_min, salary_max, salary_cur = None, None, None

        if len(salary) == 1:
            ...  # [з/п не указана]

        elif salary[0] == 'от ' and salary[2] == ' до ':
            salary_min, salary_max, salary_cur = int(salary[1].replace("\xa0", "")), salary[3].replace("\xa0", ""), salary[5]
        elif salary[0] == 'от ' and salary[0] != 'до ':
            salary_min, salary_max, salary_cur = salary[1].replace("\xa0", ""), salary_max, salary[3]
        elif salary[0] == 'до ' and salary[0] != 'от ':
            salary_min, salary_max, salary_cur = salary_min, int(salary[1].replace("\xa0", "")), salary[3]

        return {'salary_min': salary_min, 'salary_max': salary_max, 'salary_cur': salary_cur}

    def process_salary_sj(self, salary):
        salary_min, salary_max, salary_cur = None, None, None

        if len(salary) == 1:
            ...  # [По договоренности]

        elif salary[0] == 'от ' and salary[2] == ' до ':
            salary_min, salary_max, salary_cur = int(salary[2].replace("\xa0", "")), salary[3].replace("\xa0", ""), salary[5]
        elif salary[0] == 'от':
            salary_min, salary_max, salary_cur = int(salary[2][:-4].replace("\xa0", "")), salary_max, salary[2][-4:]
        elif salary[0] == 'до':
            salary_min, salary_max, salary_cur = salary_min, int(salary[2][:-4].replace("\xa0", "")), salary[2][-4:]
        else:
            salary_min, salary_max, salary_cur = int(salary[0].replace("\xa0", "")), int(salary[0].replace("\xa0", "")), salary[2]
        return {'salary_min': salary_min, 'salary_max': salary_max, 'salary_cur': salary_cur}
