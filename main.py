import psycopg2

import db_conn


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE Emails;
            DROP TABLE Phones;
            DROP TABLE Users;
            """)
        conn.commit()
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            First_name varchar(20) not null,
            Last_name varchar(20) not null);
            """)
        cur.execute("""CREATE TABLE IF NOT EXISTS emails (
            email_id SERIAL PRIMARY KEY,
            email varchar(50) not null, 
            user_id integer not null references Users(user_id));
            """)
        cur.execute("""CREATE TABLE IF NOT EXISTS phones (
            phone_id SERIAL PRIMARY KEY,
            phone bigint not null, 
            user_id integer not null references Users(user_id));
            """)
        conn.commit()

def new_client(conn, First_name, Last_name, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM users WHERE First_name = %s AND Last_name = %s;
            """, (First_name, Last_name))
        user_info = cur.fetchall()
        
        
        if len(user_info) == 0:
            cur.execute("""
                INSERT INTO users (First_name, Last_name) VALUES(%s, %s) RETURNING user_id;
                """, (First_name, Last_name))
            user_id = cur.fetchone()[0]
            conn.commit() # В лекции говорилось что комит не нужен если есть fetchone или другие fetch, но на деле почему-то id пользователя и вправду генерится и возвращается, но без комита в базе запись эта не появляется

            if email is not None:
                cur.execute("""
                    INSERT INTO emails (email, user_id) VALUES(%s, %s);
                    """, (email, user_id))
                conn.commit()

            if phone is not None:
                cur.execute("""
                    INSERT INTO phones (phone, user_id) VALUES(%s, %s);
                    """, (phone, user_id))
                conn.commit()
        else:
            user_id = user_info[0][0]
            print(f"Пользователь '{First_name}' '{Last_name}' уже существует в базе под user_id={user_id}")

def add_phone(conn, First_name, Last_name, phone):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM users WHERE First_name = %s AND Last_name = %s;
            """, (First_name, Last_name))
        user_info = cur.fetchall()
        if len(user_info) == 0:
            print(f"Пользователь '{First_name}' '{Last_name}' отсутствует в базе")
        else:
            user_id = user_info[0][0]
            cur.execute("""
                INSERT INTO phones (phone, user_id) VALUES(%s, %s);
                """, (phone, user_id))
            conn.commit()

def change_user_info(conn, First_name_old, Last_name_old, First_name=None, Last_name=None, email=None, phone=None):
    if First_name is None and Last_name is None and email is None and phone is None:
        print("Необходимо заполнить как минимум 1 поле из First_name, Last_name, email, phone")
    else:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM users WHERE First_name = %s AND Last_name = %s;
                """, (First_name_old, Last_name_old))
            user_info = cur.fetchall()
            if len(user_info) == 0:
                print(f"Пользователь '{First_name_old}' '{Last_name_old}' отсутствует в базе")
            else:
                user_id = user_info[0][0]
                if First_name is not None:
                    cur.execute("""
                        UPDATE users
                        SET First_name=%s
                        WHERE user_id=%s;
                        """, (First_name, user_id))
                    conn.commit()
                if Last_name is not None:
                    cur.execute("""
                        UPDATE users
                        SET Last_name=%s
                        WHERE user_id=%s;
                        """, (Last_name, user_id))
                    conn.commit()
                if email is not None:
                    cur.execute("""
                        UPDATE emails
                        SET email=%s
                        WHERE user_id=%s;
                        """, (email, user_id))
                    conn.commit()
                if phone is not None:
                    cur.execute("""
                        UPDATE phones
                        SET phone=%s
                        WHERE user_id=%s;
                        """, (phone, user_id))
                    conn.commit()

def del_phone(conn, First_name, Last_name, phone):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM users WHERE First_name = %s AND Last_name = %s;
            """, (First_name, Last_name))
        user_info = cur.fetchall()
        if len(user_info) == 0:
            print(f"Пользователь '{First_name}' '{Last_name}' отсутствует в базе")
        else:
            user_id = user_info[0][0]
            cur.execute("""
                DELETE FROM phones
                WHERE phone=%s AND user_id=%s;
                """, (phone, user_id))
            conn.commit()

def del_user(conn, First_name, Last_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM users WHERE First_name = %s AND Last_name = %s;
            """, (First_name, Last_name))
        user_info = cur.fetchall()
        if len(user_info) == 0:
            print(f"Пользователь '{First_name}' '{Last_name}' отсутствует в базе")
        else:
            user_id = user_info[0][0]
            cur.execute("""
                DELETE FROM phones
                WHERE user_id=%s;
                """, (user_id,))
            conn.commit()
            cur.execute("""
                DELETE FROM emails
                WHERE user_id=%s;
                """, (user_id,))
            conn.commit()
            cur.execute("""
                DELETE FROM users
                WHERE user_id=%s;
                """, (user_id,))
            conn.commit()

def find_user(conn, First_name=None, Last_name=None, email=None, phone=None):
     if First_name is None and Last_name is None and email is None and phone is None:
        print("Необходимо заполнить как минимум 1 поле из First_name, Last_name, email, phone")
     else:
        with conn.cursor() as cur:
            print()
            cur.execute("""
                SELECT u.*, e.email, p.phone
                FROM users u 
                LEFT JOIN emails e ON e.user_id = u.user_id
                LEFT JOIN phones p ON p.user_id = u.user_id;
                """)
            user_info = cur.fetchall()
            
            filtered_user_info = ''
            for el in user_info:
                if (First_name is None or el[1] == First_name) and (Last_name is None or el[2] == Last_name) and (email is None or el[3] == email) and (phone is None or el[4] == phone):
                    filtered_user_info += f'user_id - {el[0]}, Имя - {el[1]}, Фамилия - {el[2]}, email - {el[3]}, телефон - {el[4]}\n'
            
            if filtered_user_info == '':
                print('Пользователь не найден')
            else:
                print(filtered_user_info)
           

if __name__ == "__main__":
    conn = psycopg2.connect(database="postgres", user = db_conn.user, password=db_conn.password)
    
    create_db(conn)
    new_client(conn, First_name="Иван", Last_name="Иванов")
    new_client(conn, First_name="Иван", Last_name="Иванов")
    new_client(conn, First_name="Петр", Last_name="Петров", email="petrov_p@mail.ru")
    new_client(conn, First_name="Сидр", Last_name="Сидоров", email="cidorov_c@mail.ru", phone=89999999999)

    add_phone(conn, First_name="Николай", Last_name="Петров", phone=89999999998)
    add_phone(conn, First_name="Петр", Last_name="Петров", phone=89999999998)

    change_user_info(conn, First_name_old="Николай", Last_name_old="Петров", Last_name="Николов")
    change_user_info(conn, First_name_old="Петр", Last_name_old="Петров", Last_name="Николов")

    del_phone(conn, First_name="Петр", Last_name="Петров", phone=89999999998)
    del_phone(conn, First_name="Петр", Last_name="Николов", phone=89999999998)

    del_user(conn, First_name="Петр", Last_name="Николов")

    find_user(conn, First_name="Иван")

    conn.close()
