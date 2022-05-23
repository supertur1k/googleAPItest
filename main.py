import urllib.request
from xml.dom import minidom
from config import host, user, password, db_name

import httplib2
import apiclient
import psycopg2
from oauth2client.service_account import ServiceAccountCredentials


def get_exchange_rates():
    url = 'http://www.cbr.ru/scripts/XML_daily.asp'
    web_file = urllib.request.urlopen(url)
    xml_response = web_file.read()
    dom = minidom.parseString(xml_response)
    dom.normalize()
    elements = dom.getElementsByTagName("Valute")

    exchange_rates_dict = {}

    for node in elements:
        for child in node.childNodes:
            if child.nodeType == 1:
                if child.tagName == "Value":
                    if child.firstChild.nodeType == 3:
                        value = float(child.firstChild.data.replace(',', '.'))
                if child.tagName == "CharCode":
                    if child.firstChild.nodeType == 3:
                        char_code = child.firstChild.data
        exchange_rates_dict[char_code] = value

    return exchange_rates_dict


# range_to - крайняя правая ячейка для диапазона данных
def get_values_from_sheets(CREDENTIALS_FILE, spreadsheet_id, range_to):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

    apiResponse = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='A1:' + range_to,
        majorDimension='ROWS'
    ).execute()

    values = apiResponse.get('values')
    return values


def add_rubles_column(values):
    rates = get_exchange_rates()
    usd = rates.get('USD')

    values[0].append("Стоимость, RUB")
    for i in range(1, len(values)):
        values[i].append(float(values[i][2]) * usd)

    return values


def create_table():
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )

        cursor = connection.cursor()

        cursor.execute("""CREATE TABLE orders(
            order_id INT NOT NULL,
            cost_dollars DOUBLE PRECISION NOT NULL,
            delivery_date DATE NOT NULL,
            cost_rub DOUBLE PRECISION NOT NULL)""")

        connection.commit()

        print("Database created")

    except Exception as _ex:
        print("error with db connection: ", _ex)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert(num_order_func, cost_dollars_func, date_delivery_func, cost_rub_func):
    try:
        conn = psycopg2.connect("dbname='google' user='postgres' password='artur' host='localhost' port='5432'")
        cur = conn.cursor()
        cur.execute("INSERT INTO orders (order_id, cost_dollars, delivery_date, cost_rub) "
                "VALUES('%s','%s','%s','%s')" %
                (num_order_func, cost_dollars_func, date_delivery_func, cost_rub_func))
        conn.commit()
    except Exception as _ex:
        print("Error with db insert: ", _ex)
    finally:
        if conn:
            cur.close()
            conn.close()

    conn.close()


def place_data_in_database(data):
    create_table()
    for i in range(1, len(data)):
        num_order = int(data[i][1])
        cost_dollars = float(data[i][2])
        date = (data[i][3])
        day, month, year = date.split('.')
        date = year + "-" + month + "-" + day
        cost_rub = float(data[i][4])
        insert(num_order, cost_dollars, date, cost_rub)


if __name__ == '__main__':
    values_from_sheets = get_values_from_sheets('creds.json', '1Bq_6kAo5wtfaq6tE8xw6qar9mLWArJV6rikRahVtCA8', "D100")
    add_rubles_column(values_from_sheets)
    place_data_in_database(values_from_sheets)
