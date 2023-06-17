-- Создание пользователя user1 и присвоение ему
-- привилегий DBA (Database Administrator)
CREATE USER user1 IDENTIFIED BY user1;
GRANT DBA TO user1;

-- Создание пользователя user2 и предоставление ему
-- прав на создание сессии в БД и вставки данных в любую таблицу БД
CREATE USER user2 IDENTIFIED BY user2;
GRANT CREATE SESSION TO user2;
GRANT INSERT ANY TABLE TO user2;

-- Создание таблицы студентов
CREATE TABLE students (
  id NUMBER,
  name VARCHAR(50),
  group_number NUMBER(10),
  CONSTRAINT students_pk PRIMARY KEY (id)
);

-- Создание таблицы достижений
CREATE TABLE achievements(
  id NUMBER,
  name VARCHAR(60),
  cost NUMBER(10),
  partition VARCHAR(30),
  student_id NUMBER,
  CONSTRAINT achievements_pk PRIMARY KEY (id),
  CONSTRAINT achievements_fk FOREIGN KEY (student_id) REFERENCES students(id)
);

-- Создание sequence для студентов.
-- P.S. Sequence - это объект базы данных, который генерирует
-- уникальные числовые значения по определенной последовательности.
CREATE SEQUENCE sequence_students;

-- Создание триггера для операции вставки новых данных в таблицу students
CREATE OR REPLACE TRIGGER trigger_new_student
    BEFORE INSERT ON students
    FOR EACH ROW
BEGIN
  IF :new.id IS NULL THEN :new.id:=sequence_students.NEXTVAL;
  END IF;
END;

-- Вставка тестового студента и вывод таблицы students
INSERT INTO students (name, group_number) VALUES ('Акакий Половой', 4542);
SELECT * FROM students

-- Аналогично как для студентов, только для достижений
CREATE SEQUENCE sequence_achievements;
CREATE OR REPLACE TRIGGER trigger_new_achievement
    BEFORE INSERT ON achievements
    FOR EACH ROW
BEGIN
  IF :new.id IS NULL THEN :new.id:=sequence_achievements.NEXTVAL;
  END IF;
END;

INSERT INTO achievements (name, cost, partition, student_id) VALUES ('Мастер спорта', 10000, 'бокс', 1);
SELECT * FROM achievements

-- Создание таблицы логов при взаимодействии с достижениями
CREATE TABLE achievement_logs(
  id NUMBER,
  achievement_id NUMBER,
  achievement_name VARCHAR(60),
  achievement_cost NUMBER(10),
  achievement_partition VARCHAR(30),
  student_id NUMBER,
  who VARCHAR(60),
  what NUMBER,
  whenn DATE,
  CONSTRAINT logs_pk PRIMARY KEY (id)
);

-- Аналогично как для студентов, только для логов достижений
CREATE SEQUENCE sequence_logs;
CREATE OR REPLACE TRIGGER trigger_new_achievement_log
    BEFORE INSERT ON achievement_logs
    FOR EACH ROW
BEGIN
  IF :new.id IS NULL THEN
    :new.id:=sequence_logs.NEXTVAL;
  END IF;
END;

-- Создание триггера при изменении достижений
CREATE OR REPLACE TRIGGER trigger_fix_achievement
    AFTER INSERT OR UPDATE OR DELETE ON achievements
    FOR EACH ROW
DECLARE
    v_action_type NUMBER;
BEGIN
    IF INSERTING THEN
        v_action_type := 1;
    ELSIF UPDATING THEN
        v_action_type := 2;
    ELSE
        v_action_type := 3;
    END IF;

    INSERT INTO achievement_logs (
        achievement_id,
        achievement_name,
        achievement_cost,
        achievement_partition,
        student_id,
        who,
        what,
        whenn
    ) VALUES (
        :new.id,
        :new.name,
        :new.cost,
        :new.partition,
        :new.student_id,
        USER,
        v_action_type,
        SYSDATE
    );

END;

-- Создание временной таблицы для отчётов.
-- P.S. Временные таблицы - это таблицы, которые хранят данные только на время выполнения сеанса пользователя, который ее создал.
-- Эти таблицы автоматически удаляются при завершении транзакции или сеанса.
CREATE GLOBAL TEMPORARY TABLE temp_report (
    id INTEGER,
    username VARCHAR(30),
    operation_name VARCHAR(20),
    operation_date DATE,
    achievements_id INTEGER,
    commentary VARCHAR(4000)
) ON COMMIT PRESERVE ROWS;

-- Изменение значений достижения 1
UPDATE achievements
SET cost = 12500
WHERE id = 1

UPDATE achievements
SET cost = 7600
WHERE id = 1

SELECT * FROM achievement_logs

-- Процедура заполнения отчёта
CREATE OR REPLACE PROCEDURE BuildReport (date1 DATE, date2 DATE) AS
    commentary VARCHAR(4000);
    old_achievement_id NUMBER;
    old_achievement_name VARCHAR(60);
    old_achievement_cost NUMBER(10);
    old_achievement_partition VARCHAR(30);
    old_student_id NUMBER;
    old_student_name VARCHAR(50);
    new_student_name VARCHAR(50);

BEGIN
    FOR R IN (SELECT DISTINCT achievement_id FROM achievement_logs WHERE whenn BETWEEN date1 AND date2) LOOP
        old_achievement_id := NULL;
        old_achievement_name := NULL;
        old_achievement_cost := NULL;
        old_achievement_partition := NULL;
        old_student_id := -1;


        FOR L IN (SELECT id, achievement_id, achievement_name, achievement_cost, achievement_partition, student_id, who, what, whenn FROM achievement_logs WHERE achievement_id = R.achievement_id AND whenn BETWEEN date1 AND date2 ORDER BY whenn) LOOP
            commentary := L.whenn||' '||
            CASE
                WHEN L.what=1 THEN 'Добавлено'
                WHEN L.what=2 THEN 'Отредактировано'
                WHEN L.what=3 THEN 'Удалено'
            END;

            IF NVL(L.achievement_name, '-598') <> NVL(old_achievement_name, '-598') THEN
                commentary := commentary||' Название достижения: было - '||old_achievement_name||' стало- '||L.achievement_name;
            END IF;

            IF NVL(L.achievement_cost, '-598') <> NVL(old_achievement_cost, '-598') THEN
                commentary := commentary||' Оплата за достижение: была - '||old_achievement_cost||' стала- '||L.achievement_cost;
            END IF;

            IF NVL(L.achievement_partition, '-598') <> NVL(old_achievement_partition, '-598') THEN
                commentary := commentary||' Группа достижения: была - '||old_achievement_partition||' стала- '||L.achievement_partition;
            END IF;

            IF NVL(old_student_id, -1) <> NVL(L.student_id, -1) THEN
                BEGIN
                    SELECT name INTO old_student_name FROM students WHERE id=old_student_id;
                EXCEPTION
                    WHEN OTHERS THEN
                        old_student_name := '';
                END;
                BEGIN
                    SELECT name INTO new_student_name FROM students WHERE id=L.student_id;
                EXCEPTION
                    WHEN OTHERS THEN
                        new_student_name := '';
                END;

                commentary := commentary||' Имя студента: было - '||old_student_name||' стало - '||new_student_name;

            END IF;

            INSERT INTO temp_report(commentary) values (commentary);
            old_achievement_id := L.achievement_id;
            old_achievement_name := L.achievement_name;
            old_achievement_cost := L.achievement_cost;
            old_achievement_partition := L.achievement_partition;
            old_student_id := L.student_id;
        END LOOP;
    END LOOP;
END;

-- Запуск генерации отчёта
BEGIN BuildReport(sysdate -30, sysdate);
END;
SELECT * FROM temp_report

-- Создание пакета для отчёта
CREATE OR REPLACE PACKAGE report_package AS
    TYPE report_row IS RECORD (
        report_id NUMBER,
        who VARCHAR(30),
        what VARCHAR(20),
        whenn DATE,
        achievement_id NUMBER,
        commentary VARCHAR(4000)
    );
    TYPE report_table is table of report_row;
END;

CREATE OR REPLACE FUNCTION BuildReport2 (date1 DATE, date2 DATE) RETURN report_package.report_table PIPELINED AS
    rec report_package.report_row;
    commentary VARCHAR(4000);
    old_achievement_id NUMBER;
    old_achievement_name VARCHAR(60);
    old_achievement_cost NUMBER(10);
    old_achievement_partition VARCHAR(30);
    old_student_id NUMBER;
    old_student_name VARCHAR(50);
    new_student_name VARCHAR(50);
BEGIN
    FOR R IN (SELECT DISTINCT achievement_id FROM achievement_logs WHERE whenn BETWEEN date1 AND date2) LOOP
        old_achievement_id := NULL;
        old_achievement_name := NULL;
        old_achievement_cost := NULL;
        old_achievement_partition := NULL;
        old_student_id := -1;

        FOR L IN (SELECT id, achievement_id, achievement_name, achievement_cost, achievement_partition, student_id, who, what, whenn FROM achievement_logs WHERE achievement_id = R.achievement_id AND whenn BETWEEN date1 and date2 ORDER BY whenn) LOOP
            commentary := L.whenn||' '||
            CASE
                WHEN L.what=1 THEN 'Добавлено'
                WHEN L.what=2 THEN 'Отредактировано'
                WHEN L.what=3 THEN 'Удалено'
            END;

            IF NVL(L.achievement_name, '-598') <> NVL(old_achievement_name, '-598') THEN
                commentary := commentary||' Название достижения: было - '||old_achievement_name||' стало- '||L.achievement_name;
            END IF;

            IF NVL(L.achievement_cost, '-598') <> NVL(old_achievement_cost, '-598') THEN
                commentary := commentary||' Оплата за достижение: была - '||old_achievement_cost||' стала- '||L.achievement_cost;
            END IF;

            IF NVL(L.achievement_partition, '-598') <> NVL(old_achievement_partition, '-598') THEN
                commentary := commentary||' Группа достижения: была - '||old_achievement_partition||' стала- '||L.achievement_partition;
            END IF;

            IF NVL(old_student_id, -1) <> NVL(L.student_id, -1) THEN
                BEGIN
                    SELECT name INTO old_student_name FROM students WHERE id=old_student_id;
                EXCEPTION
                    WHEN OTHERS THEN
                        old_student_name := '';
                END;
                BEGIN
                    SELECT name INTO new_student_name FROM students WHERE id=L.student_id;
                EXCEPTION
                    WHEN OTHERS THEN
                        new_student_name := '';
                END;

                commentary := commentary||' Имя студента: было - '||old_student_name||' стало - '||new_student_name;

            END IF;

            old_achievement_id := L.achievement_id;
            old_achievement_name := L.achievement_name;
            old_achievement_cost := L.achievement_cost;
            old_achievement_partition := L.achievement_partition;
            old_student_id := L.student_id;
            rec.commentary := commentary;
            pipe row ( rec );

        END LOOP;
    END LOOP;
END;

SELECT * FROM TABLE ( BuildReport2(sysdate -30, sysdate) );

CREATE TABLE alert (commentary VARCHAR(100));

CREATE OR REPLACE PROCEDURE write_alert (commentary VARCHAR(100)) AS PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO alert values (commentary);
        COMMIT;
    END;

-- Создание триггера для проверки имени пользователя),
-- который выполняет операцию вставки, обновления или удаления в таблице achievements.
-- Если имя пользователя равно 'user1', то триггер вызывает процедуру write_alert,
-- чтобы записать сообщение "Взлом" в таблицу alert.
-- Затем триггер вызывает функцию raise_application_error,
-- которая генерирует ошибку с кодом -20000 и текстом "Нельзя",
-- что приводит к отмене выполнения операции вставки, обновления или удаления.
CREATE OR REPLACE TRIGGER trigger_vzlom
    BEFORE INSERT OR UPDATE OR DELETE ON achievements FOR EACH ROW
    BEGIN
        IF UPPER(USER) = UPPER('user1') THEN
            write_alert('Взлом');
            raise_application_error(-20000, 'Нельзя');
        END IF;
    END;





