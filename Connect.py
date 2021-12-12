import psycopg2
from psycopg2 import sql, extensions
# Конфигурация базы данных
host = "127.0.0.1"
user = "postgres"
password = "123"
db_name = "Homework"

class DataBase():

    def __init__(self, host, user, password, db_name):
        #Сохраняем соединение с БД
        self.connect = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
    )
        self.slovar = {
            "fam": ["f_id", "f_val", "lastname"],
            "name": ["n_id", "n_val", "name"],
            "otez": ["ot_id", "ot_val", "otchestvo"],
            "street": ["st_id", "st_val", "street"]
        }

    def __del__(self):
        #Закрываем соединение
        self.connect.close()

    def find(self, value, colomn, value_id, table):
        #Находим элемент из БД возвращаем его в виде кортежа
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT {extensions.quote_ident(value_id, cursor)} FROM {extensions.quote_ident(table, cursor)}
                WHERE {extensions.quote_ident(colomn, cursor)} = %s
                """,
                [value]
            ) 
            val = cursor.fetchone()
            # if val:
            #     print(val)
            # else:
            #     print("Not find")
            return val

    # def update_parent(self, value, colomn_value, main_id):
    #     with self.connect.cursor() as cursor:
    #         cursor.execute(
    #             f"""
    #             SELECT {extensions.quote_ident(self.slovar[colomn_value][0], cursor)} from {extensions.quote_ident(self.slovar[colomn_value][2], cursor)}
    #             WHERE {extensions.quote_ident(self.slovar[colomn_value][1], cursor)} = %s
    #             """,
    #             [value]
    #         )
    #         id = cursor.fetchone()
    #         if id:
    #             self.update_main(id, colomn_value, main_id)
    #         else:
    #             id = self.add(value, self.slovar[colomn_value][1], self.slovar[colomn_value][0], self.slovar[colomn_value][2])
    #             self.update_main(id, colomn_value, main_id)

    def update_main(self, value, colomn_value, main_id):
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE main SET {extensions.quote_ident(colomn_value, cursor)} = %s
                WHERE id = %s
                """,
                [value, main_id]
            )
            self.connect.commit()

    def delete(self, main_id):
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                DELETE FROM main 
                WHERE id = %s
                """,
                [main_id]
            )
            self.connect.commit()

    def deleteParent(self, table, colomn_id, id):
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                DELETE FROM {extensions.quote_ident(table, cursor)}
                WHERE {extensions.quote_ident(colomn_id, cursor)} = %s
                """,
                [id]
            )
            self.connect.commit()

    def showTable(self):
        #Показывает основную таблицу БД
        with self.connect.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
                JOIN lastname ON f_id = fam
                JOIN name ON n_id = name
                JOIN otchestvo ON ot_id = otez
                JOIN street ON st_id = street
                ORDER BY id;
                """
            )
            # for row in cursor.fetchall():
            #     for element in row:
            #         print(element, end=" ")
            #     print()
            return cursor.fetchall()

    def showTableIf(self, colomn_value, value):
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT {extensions.quote_ident(self.slovar[colomn_value][0], cursor)} from {extensions.quote_ident(self.slovar[colomn_value][2], cursor)}
                WHERE {extensions.quote_ident(self.slovar[colomn_value][1], cursor)} = %s
                """,
                [value]
            )  
            id = cursor.fetchone()          
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value, cursor)} = %s
            """,
            [id]
            )
            return cursor.fetchall()
        
    def show1(self, colomn_value, value):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value, cursor)} = %s
            """,
            [value]
            )  
            return cursor.fetchall()


    def showOr1(self, colomn_value1, value1, colomn_value2, value2):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value1, cursor)} = %s 
            OR {extensions.quote_ident(colomn_value2, cursor)} = %s
            """,
            [value1, value2]
            )
            return cursor.fetchall()

    def showOr2(self, colomn_value1, value1, colomn_value2, value2, colomn_value3, value3):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value1, cursor)} = %s 
            OR {extensions.quote_ident(colomn_value2, cursor)} = %s
            OR {extensions.quote_ident(colomn_value3, cursor)} = %s
            """,
            [value1, value2, value3]
            )
            return cursor.fetchall()

    def showAnd1(self, colomn_value1, value1, colomn_value2, value2):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value1, cursor)} = %s 
            AND {extensions.quote_ident(colomn_value2, cursor)} = %s
            """,
            [value1, value2]
            )
            return cursor.fetchall()

    def showAnd2(self, colomn_value1, value1, colomn_value2, value2, colomn_value3, value3):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value1, cursor)} = %s 
            AND {extensions.quote_ident(colomn_value2, cursor)} = %s
            AND {extensions.quote_ident(colomn_value3, cursor)} = %s
            """,
            [value1, value2, value3]
            )
            return cursor.fetchall()

    def showOr1And1(self, colomn_value1, value1, colomn_value2, value2, colomn_value3, value3):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value1, cursor)} = %s 
            OR {extensions.quote_ident(colomn_value2, cursor)} = %s
            AND {extensions.quote_ident(colomn_value3, cursor)} = %s
            """,
            [value1, value2, value3]
            )
            return cursor.fetchall()

    def showAnd1Or1(self, colomn_value1, value1, colomn_value2, value2, colomn_value3, value3):
        with self.connect.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT id, f_val, n_val, ot_val, st_val, dom, dom_k, flat, phone FROM main
            JOIN lastname ON f_id = fam
            JOIN name ON n_id = name
            JOIN otchestvo ON ot_id = otez
            JOIN street ON st_id = street
            WHERE {extensions.quote_ident(colomn_value1, cursor)} = %s 
            AND {extensions.quote_ident(colomn_value2, cursor)} = %s
            OR {extensions.quote_ident(colomn_value3, cursor)} = %s
            """,
            [value1, value2, value3]
            )
            return cursor.fetchall()         

    def showParent(self, table):
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM {extensions.quote_ident(table, cursor)}
                """
            )
            return cursor.fetchall()

    def add(self, value, colomn_value, value_id, table):
        #Метод добавляет значение в родительскую таблицу и возвращает его id 
        with self.connect.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {extensions.quote_ident(table, cursor)} VALUES(default, %s);
                """,
                [value]
            )
            self.connect.commit()
            cursor.execute(
                f"""
                SELECT {extensions.quote_ident(value_id, cursor)} FROM {extensions.quote_ident(table, cursor)}
                WHERE {extensions.quote_ident(colomn_value, cursor)}= %s
                """,
                [value]
            )
            return cursor.fetchone()
        

    def insert(self, names):
        tables = (
            ('f_val', 'f_id','lastname'), 
            ('n_val', 'n_id','name'), 
            ('ot_val', 'ot_id','otchestvo'),
            ('st_val', 'st_id','street')
            )
        i = 0
        val_id = []

        for row in tables:
            val_id.append(self.find(names[i], row[0], row[1], row[2]))
            if not val_id[i]:
                val_id[i] = self.add(names[i], row[0], row[1], row[2])
            i += 1
        with self.connect.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO main VALUES(
                    default,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                [
                    val_id[0][0], #Фамилия
                    val_id[1][0], #Имя
                    val_id[2][0], #Отчество
                    val_id[3][0], #Улица
                    names[4],     #Дом
                    names[5],     #Подъезд
                    names[6],     #Квартира
                    names[7]      #Номер телефона
                ]

            )
            self.connect.commit()
        

            

if __name__ == "__main__":
    db = DataBase(host, user, password, db_name)
    # print(db.showTableIf("fam", "Rogo"))
    # db.insert(["Tilov", "Iliya", "Sergeivich", "Sovetskaya", "34", "12", "67", "89456748697"])
    # db.delete("7")
    # db.update_parent("Nikusa", "name", "1")
    # print(db.find("Nikita", "n_val", 'n_id', "name"))
    # print(db.showParent("name"))
    # print(db.showOr1('n_val', 'Nikita','f_val', 'Petrov'))