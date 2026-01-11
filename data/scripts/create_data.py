import random
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from pathlib import Path
import pandas as pd

from faker import Faker
import faker_microservice


SEED = 0

def generate_categories(cnt: int, levels: int=2, start_sk: int=1) -> list:
    rng = random.Random()
    fake = Faker("ru_RU")
    fake.seed_instance(SEED)
    fake.add_provider(faker_microservice.Provider)

    categories = []
    idx = start_sk
    count_on_lvl = cnt
    while cnt > 0 and levels > 0:
        count_on_lvl = count_on_lvl // 10 if levels > 1 and count_on_lvl > 10 else cnt
        cnt -= count_on_lvl
        batch = []
        for _ in range(count_on_lvl):
            temp = {
                "sk": idx,
                "bk": fake.unique.microservice(),
                "category": rng.choice(categories)["sk"] if categories else '',
                "description": fake.sentence(nb_words=rng.randint(5, 15)),
                }
            idx += 1
            batch.append(temp)
        categories.extend(batch)
        levels -= 1
    return categories
    


def generate_products(cnt: int, categories: list, start_sk: int=1):
    rng = random.Random(SEED)
    fake = Faker('ru_RU')
    fake.seed_instance(SEED)

    childs = {}
    for cat in categories:
        childs[cat['sk']] = []
    for cat in categories:
        if cat['category']:
            childs[cat['category']].append(cat['sk'])
    leaves = list(filter(lambda cat: not childs[cat['sk']], categories))

    ltrs = ['У', 'К', 'К', 'К', 'Н', 'Е', 'Г', 'Ш', 'Ш', 'У', 'З', 'Х', 'Х',
            'Х', 'Х', 'Х', 'Ф', 'В', 'А', 'П', 'П', 'П', 'П', 'О', 'Л',
            'Ж', 'Э', 'Я', 'Ч', 'С', 'С', 'М', 'М', 'М', 'М', 'М', 'М', 'М',
            'Т', 'Б', 'Ю']

    products = []
    for idx in range(start_sk, start_sk + cnt):
        bk = str(rng.randint(1,10))
        for _ in range(rng.randint(1,3)):
            bk += rng.choice(ltrs)
        if rng.random() < 0.5:
            bk += "-" + str(rng.randint(10, 100))
        else:
            bk += "." + str(rng.randint(1, 10)) + rng.choice(ltrs)
        bk += f".{rng.randint(0, 10000):04d}-{rng.randint(1, 100):02d}"  
        products.append({
            "sk": idx,
            "bk": bk,
            "category": rng.choice(leaves)["sk"],
            "description": fake.sentence(nb_words=rng.randint(10, 20)),
            "service_time": f"{rng.randint(3, 25)} years",
        })
    return products

def generate_clients(cnt: int, start_sk: int=1) -> list:
    rng = random.Random(SEED)
    fake = Faker("ru_RU")
    fake.seed_instance(SEED)   

    ltrs = ['У', 'К', 'Н', 'Е', 'Г', 'Ш', 'У', 'З', 'Х', 'А', 'А', 'А', 'А',
            'Ф', 'В', 'А', 'П', 'П', 'П', 'П', 'О', 'Л', 'О', 'О', 'О', 'О',
            'Ж', 'Э', 'Я', 'Ч', 'С', 'М', 'М', 'М', 'М', 'М', 'М', 'М', 'О',
            'Т', 'Б', 'Ю', 'М', 'М', 'М', 'М', 'Г', 'Г', 'Г', 'Г', 'Б', 'Б']
    abbrs = ["ЗАО", "ОАО", "ПАО", "АО", "НКО"]

    clients = []
    for idx in range(start_sk, start_sk + cnt):
        bk = ""
        if rng.random() < 0.7:
            bk = f"{rng.choice(abbrs)} "
        if rng.random() < 0.7:
            for _ in range(rng.randint(2, 4)):
                bk += rng.choice(ltrs)
            bk += " "
        bk += fake.word().capitalize()
        clients.append({
            "sk": idx,
            "bk": bk,
            "inn": fake.individuals_inn(),
            "ogrn": fake.individuals_ogrn(),
            "address": fake.address(),
            "email": fake.company_email(),
            "phone": fake.phone_number(),
        })
    return clients

def generate_orders(
        cnt: int, 
        clients: list, 
        start_sk: int=1, 
        end_dt: str="2025-11-11T12:30:00",
        start_dt: str="0000-00-00T16:20:00",) -> list:
    if end_dt <= start_dt:
        raise ValueError("End date must be greater than start date.")
    rng = random.Random(SEED)
    fake = Faker("ru_RU")
    fake.seed_instance(SEED)

    orders = []
    for idx in range(start_sk, start_sk + cnt):
        dt = fake.iso8601(
                tzinfo=datetime.now().astimezone().tzinfo, 
                end_datetime=datetime.fromisoformat(end_dt))
        while dt < start_dt:
            dt = fake.iso8601(
                tzinfo=datetime.now().astimezone().tzinfo, 
                end_datetime=datetime.fromisoformat(end_dt))
        closing_date = datetime.fromisoformat(dt) + relativedelta(
            years=rng.randint(0, 2), months=rng.randint(0, 11), 
            days=rng.randint(0, 31), hours=rng.randint(0, 23), 
            minutes=rng.randint(0, 60), seconds=rng.randint(0, 60))
        if closing_date > datetime.now(tz=closing_date.tzinfo):
            closing_date = None
        orders.append({
            "sk": idx,
            "bk": fake.uuid4(),
            "client": rng.choice(clients)["sk"],
            "created_at": dt,
            "closed_at": str(closing_date) if closing_date else None,
        })
    return orders

def generate_order_items(cnt: int, orders: list, products: list, start_sk: int=1) -> list:
    rng = random.Random(SEED)
    fake = Faker("ru_RU")
    fake.seed_instance(SEED)

    order_items = []
    for idx in range(start_sk, start_sk + cnt):
        order_items.append({
            "sk": idx,
            "bk": fake.uuid4(),
            "order": rng.choice(orders)["sk"],
            "product": rng.choice(products)["sk"],
            "amount": rng.randint(1, 99) * 10**rng.randint(0, 3),
            "price": Decimal(f"{rng.randint(1, 1000) * 10**rng.randint(0, 4)}.{rng.randint(0, 9)}0"),
            "discount": Decimal(rng.random()/2).quantize(Decimal("1.00")),
        })
    return order_items


def write_to_csv(data: list, fname: str, append: bool=False) -> None:
    dir = Path(__file__).resolve().parent.parent / "csv" / fname
    df = pd.DataFrame(data)
    df.to_csv(dir, index=False)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Create/add sample data files.")
    g = p.add_argument_group("Owerwrite options")
    g.add_argument("-a", "--append", action="store_true", 
                   help="Set write mode to appending data to the end if files exists.")
    g = p.add_argument_group("Data options")
    g.add_argument("-ord", "--orders", type=int, default=0,
                   help="Number of rows generated for order file.")
    g.add_argument("-oi", "--order_items", type=int, default=0,
                   help="Number of rows generated for order_item file.")
    g.add_argument("-p", "--products", type=int, default=0,
                   help="Number of rows generated for product file.")
    g.add_argument("-cat", "--categories", type=int, default=0,
                   help="Number of rows generated for category file.")
    g.add_argument("-c", "--clients", type=int, default=0,
                   help="Number of rows generated for client file.")
    args = p.parse_args()
    if args.append:
        raise NotImplementedError("Append option is not implemented yet.")
    
    if args.categories:
        categories = generate_categories(args.categories)
    else:
        categories = generate_categories(20)

    if args.products:
        products = generate_products(args.products, categories=categories)
    else:
        products = generate_products(100, categories=categories)

    if args.clients:
        clients = generate_clients(args.clients)
    else:
        clients = generate_clients(30)

    if args.orders:
        orders = generate_orders(
            args.orders, 
            clients=clients, 
            start_dt=(datetime.now() - relativedelta(years=2)).isoformat())
    else:
        orders = generate_orders(
            1000, 
            clients=clients, 
            start_dt=(datetime.now() - relativedelta(years=2)).isoformat())
        
    if args.order_items:
        order_items = generate_order_items(
            args.order_items, 
            orders=orders, 
            products=products)
    else:
        order_items = generate_order_items(10000, orders=orders, products=products)
    write_to_csv(categories, "categories.csv")
    write_to_csv(products, "products.csv")
    write_to_csv(clients, "clients.csv")
    write_to_csv(orders, "orders.csv")
    write_to_csv(order_items, "order_items.csv")
    




