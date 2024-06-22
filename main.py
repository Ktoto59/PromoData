import os
import aiohttp
import aiosqlite
import asyncio
import argparse
from dotenv import load_dotenv


load_dotenv()


async def fetch_categories_data(session, api_url):
    async with session.get(api_url) as response:
        if response.status == 200:
            return (await response.json()).get('categories', [])
        else:
            print(f"Failed to fetch data from API. Status code: {response.status}")
            return []


async def fetch_products_data(session, category_id):
    api_url = f'{URL}/api/local/v1/catalog/list?category_id={category_id}&limit=20000&offset=0&sort_type=popular'
    async with session.get(api_url) as response:
        if response.status == 200:
            return (await response.json()).get('products', [])
        else:
            print(f"Failed to fetch products data for category {category_id}. Status code: {response.status}")
            return []


async def fetch_offer_details(session, offer_id, semaphore):
    api_url = f'{URL}/api/local/v1/catalog/offers/{offer_id}/details'
    async with semaphore:
        async with session.get(api_url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch offer details for offer {offer_id}. Status code: {response.status}")
                return {}


async def create_database_and_tables(db_file, semaphore):
    async with aiosqlite.connect(db_file, timeout=120) as conn:
        cursor = await conn.cursor()

        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')

        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS subcategories (
                id INTEGER PRIMARY KEY,
                name TEXT,
                parent_id INTEGER,
                FOREIGN KEY (parent_id) REFERENCES categories(id)
            )
        ''')

        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                brand TEXT,
                subcategory_id INTEGER,
                FOREIGN KEY (subcategory_id) REFERENCES subcategories(id)
            )
        ''')

        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS offers (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                retail_price REAL,
                discount_price REAL,
                discount BOOLEAN,
                vendor_code TEXT,
                is_available BOOLEAN,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')

        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS shops (
                id INTEGER PRIMARY KEY,
                address TEXT,
                subways TEXT
            )
        ''')

        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS offer_shop_amount (
                offer_id INTEGER,
                shop_id INTEGER,
                availability_text TEXT,
                PRIMARY KEY (offer_id, shop_id),
                FOREIGN KEY (offer_id) REFERENCES offers(id),
                FOREIGN KEY (shop_id) REFERENCES shops(id)
            )
        ''')

        await conn.commit()


async def insert_category_to_db(db_file, category, semaphore):
    async with aiosqlite.connect(db_file, timeout=120) as conn:
        cursor = await conn.cursor()

        await cursor.execute('''
            INSERT OR REPLACE INTO categories (id, name)
            VALUES (?, ?)
        ''', (category['id'], category['name']))

        await conn.commit()


async def insert_subcategory_to_db(db_file, subcategory, top_category_id):
    async with aiosqlite.connect(db_file, timeout=120) as conn:
        cursor = await conn.cursor()

        await cursor.execute('''
            INSERT OR REPLACE INTO subcategories (id, name, parent_id)
            VALUES (?, ?, ?)
        ''', (subcategory['id'], subcategory['name'], top_category_id))

        await conn.commit()

        # Рекурсивная функция для обработки вложенных подкатегорий
        if 'subcategories' in subcategory:
            for subsub in subcategory['subcategories']:
                await insert_subcategory_to_db(db_file, subsub, top_category_id)


async def insert_product_to_db(db_file, product, subcategory_id, semaphore):
    async with aiosqlite.connect(db_file, timeout=120) as conn:
        cursor = await conn.cursor()

        await cursor.execute('''
            INSERT OR REPLACE INTO products (id, name, brand, subcategory_id)
            VALUES (?, ?, ?, ?)
        ''', (product['id'], product['name'], product['brand'], subcategory_id))

        await conn.commit()

        tasks = [insert_offer_to_db(db_file, offer['id'], product['id'], semaphore) for offer in product['offers']]
        await asyncio.gather(*tasks)


async def insert_offer_to_db(db_file, offer_id, product_id, semaphore):
    async with aiohttp.ClientSession() as session:
        offer_details = await fetch_offer_details(session, offer_id, semaphore)

    if not offer_details:
        return

    async with aiosqlite.connect(db_file, timeout=120) as conn:
        cursor = await conn.cursor()

        discount = float(offer_details['discount_price']) < float(offer_details['retail_price'])
        await cursor.execute('''
            INSERT OR REPLACE INTO offers (id, product_id, retail_price, discount_price, discount, vendor_code, is_available)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (offer_details['id'], product_id, offer_details['retail_price'], offer_details['discount_price'], discount,
              offer_details['vendor_code'], offer_details['availability_info']['is_available']))

        for shop in offer_details['availability_info']['offer_store_amount']:
            await cursor.execute('''
                INSERT OR REPLACE INTO shops (id, address, subways)
                VALUES (?, ?, ?)
            ''', (shop['shop_id'], shop['address'], ', '.join([subway['name'] for subway in shop['subways']])))

            await cursor.execute('''
                INSERT OR REPLACE INTO offer_shop_amount (offer_id, shop_id, availability_text)
                VALUES (?, ?, ?)
            ''', (offer_details['id'], shop['shop_id'], shop['availability']['text']))

        await conn.commit()


async def process_subcategories_and_products(session, db_file, subcategories, top_category_id, semaphore):
    for subcategory in subcategories:
        await insert_subcategory_to_db(db_file, subcategory, top_category_id)

        products_data = await fetch_products_data(session, subcategory['id'])
        tasks = [insert_product_to_db(db_file, product, subcategory['id'], semaphore) for product in products_data]
        await asyncio.gather(*tasks)

        if 'subcategories' in subcategory:
            await process_subcategories_and_products(session, db_file, subcategory['subcategories'], top_category_id,
                                                     semaphore)


async def main(db_file, max_requests):
    categories_api_url = f'{URL}/api/local/v1/catalog/categories'

    semaphore = asyncio.Semaphore(max_requests)

    async with aiohttp.ClientSession() as session:
        categories_data = await fetch_categories_data(session, categories_api_url)

        await create_database_and_tables(db_file, semaphore)

        for category in categories_data:
            print(f"Сбор категории: {category['name']}")
            await insert_category_to_db(db_file, category, semaphore)
            if 'subcategories' in category:
                await process_subcategories_and_products(session, db_file, category['subcategories'], category['id'],
                                                         semaphore)

    print("Data insertion completed.")


if __name__ == "__main__":
    try:
        URL = os.getenv('URL')
        parser = argparse.ArgumentParser(description='Асинхронный скрипт для извлечения и хранения данных из API')
        parser.add_argument('--db', type=str, default='products.db', help='Имя файла базы данных SQLite')
        parser.add_argument('-rl', '--request-limit', type=int, default=10,
                            help='Максимальное ограничение на одновременные запросы')
        args = parser.parse_args()

        asyncio.run(main(args.db, args.request_limit))

    except KeyboardInterrupt:
        print("\nВыход по команде Ctrl+C. Завершение программы...")
