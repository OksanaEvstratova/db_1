import psycopg2
import pprint

# Требуется хранить персональную информацию о клиентах:
#
# имя,
# фамилия,
# email,
# телефон.
# Сложность в том, что телефон у клиента может быть не один, а два, три и даже больше.
# А может и вообще не быть телефона, например, он не захотел его оставлять.
#
# Функция, создающая структуру БД (таблицы).
def create_db(conn):
    with conn.cursor() as cur:

        cur.execute("""
        DROP TABLE clients_phones;
        DROP TABLE clients;
        """)

        cur.execute("""
                CREATE TABLE IF NOT EXISTS clients(
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(40) not NULL,
                    last_name VARCHAR(40) not NULL,
                    email VARCHAR(40) UNIQUE not NULL
                );
                """)
        cur.execute("""
                CREATE TABLE IF NOT EXISTS clients_phones(
                    id SERIAL PRIMARY KEY,
                    clients_id INTEGER NOT NULL REFERENCES clients(id),
                    phone_number VARCHAR(15)
                    
                );
                """)
        conn.commit()

def print_result(conn, description):
    print(description)
    with conn.cursor() as cur:
        cur.execute("""
        select first_name, last_name, email, cp.phone_number 
        from clients c 
        full join clients_phones cp on  c.id = cp.clients_id
        order by c.id
        """)
        pprint.pprint(cur.fetchall())
        print('-')

# Функция, позволяющая добавить нового клиента.
def add_client(conn, first_name, last_name, email, phones=None):

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(first_name, last_name, email) VALUES(%s, %s, %s) RETURNING id;
        """,
                    (first_name, last_name, email))
        client_id = cur.fetchone()[0]

        print_result(conn, 'Добавление клиента:')

        if phones != None:
            add_phone(conn, client_id, phones)

    # conn.commit()


# Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(conn, client_id, phones):

    with conn.cursor() as cur:
        for phone_number in phones.split(','):
            cur.execute("""
             INSERT INTO clients_phones(clients_id, phone_number) VALUES(%s, %s);
             """,
                        (client_id, phone_number))
    # conn.commit()
    print_result(conn, 'Добавление номера телефона:')

# Функция, позволяющая изменить данные о клиенте.
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):

    with conn.cursor() as cur:
        cur.execute("""
        UPDATE clients 
        SET first_name=COALESCE(%s, first_name), 
            last_name=COALESCE(%s, last_name), 
            email=COALESCE(%s, email)
        WHERE id=%s
        """, (first_name, last_name, email, client_id))

        cur.execute("""
        UPDATE clients_phones 
        SET phone_number=COALESCE(%s, phone_number)
        WHERE clients_id=%s
        """, (phones, client_id))

    # conn.commit()
    print_result(conn, 'Изменение клиента:')

# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(conn, client_id, phone=None):

    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM clients_phones 
                WHERE clients_id=%s and phone_number=%s;
                """, (client_id, phone))

    # conn.commit()
    print_result(conn, 'Удаление номера телефона:')

# Функция, позволяющая удалить существующего клиента.
def delete_client(conn, client_id):

    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM clients_phones 
                WHERE clients_id=%s;
                """, (client_id,))
        cur.execute("""
                DELETE FROM clients 
                WHERE id=%s;
                """, (client_id,))

    # conn.commit()
    print_result(conn, 'Удаление клиента:')

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):

    print('Результат поиска')
    with conn.cursor() as cur:
        cur.execute("""
        select first_name, last_name, email, cp.phone_number 
        from clients c 
        full join clients_phones cp on  c.id = cp.clients_id
        where (first_name = %s or %s is Null)  
            and (last_name = %s or %s is Null)     
            and (email = %s or %s is Null)
            and (cp.phone_number = %s or %s is Null)        
        order by c.id

        """, (first_name, first_name, last_name, last_name, email, email, phone, phone))
        pprint.pprint(cur.fetchall())
        print('-')




with psycopg2.connect(database="clients_db", user="postgres", password="694140") as conn:

    create_db(conn)

    add_client(conn, 'Ivanov', 'Ivan', 'i.ivanov@ya.ru', phones=None)
    add_client(conn, 'Petrov', 'Petr', 'p.petrov@ya.ru', '+79141232222')
    add_client(conn, 'Petrov', 'Akakiy', 'akakiy_foreva@mail.ru', '+79016669999')
    add_client(conn, 'Sidorov', 'Sidor', 's.sidorov@ya.ru', '+79141113333,+79142223333')


    add_phone(conn, 1, '+12345551111')

    # change_client(conn, 2, first_name='Piter', last_name='Petroff', email='p.petroff@gmail.com', phones=None)
    change_client(conn, 2, first_name=None, last_name=None, email='p.petroff@gmail.com', phones='+12223335555')

    delete_phone(conn, 4, '+79141113333')

    delete_client(conn, 1)

    find_client(conn, first_name='Petrov', last_name=None, email=None, phone=None)
    find_client(conn, first_name=None, last_name=None, email='s.sidorov@ya.ru', phone=None)
    find_client(conn, first_name='Ivanov', last_name=None, email=None, phone=None)
    find_client(conn, first_name='Petrov', last_name='Akakiy', email=None, phone=None)

conn.close()