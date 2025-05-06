-- Этап 1. Создание и заполнение БД
-- СХЕМА raw_data и загрузка CSV
-- --------------------------
CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE raw_data.sales (
    person_name TEXT,                               -- имя покупателя
    phone_number TEXT,                              -- телефон
    car_brand TEXT,                                 -- бренд машины
    brand_origin TEXT,                              -- страна происхождения бренда
    car_model TEXT,                                 -- модель
    car_color TEXT,                                 -- цвет (может быть несколько через запятую)
    car_year INT,                                   -- год выпуска
    car_price NUMERIC(9,2),                         -- цена без скидки
    discount NUMERIC(5,2),                          -- скидка в процентах
    gasoline_consumption NUMERIC(4,1),              -- расход топлива, может быть NULL
    sale_date DATE                                  -- дата покупки
);

-- Загрузка CSV-файла (вручную в DBeaver или через psql):
-- \\copy raw_data.sales FROM 'cars (1).csv' DELIMITER ',' CSV HEADER;

-- --------------------------
-- СХЕМА car_shop и нормализованные таблицы
-- --------------------------
CREATE SCHEMA IF NOT EXISTS car_shop;

CREATE TABLE car_shop.brand (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,                          -- название бренда
    origin VARCHAR NOT NULL                         -- страна происхождения
);

CREATE TABLE car_shop.model (
    id SERIAL PRIMARY KEY,
    brand_id INT REFERENCES car_shop.brand(id),     -- внешний ключ на бренд
    name VARCHAR NOT NULL                           -- название модели
);

CREATE TABLE car_shop.person (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,                          -- имя покупателя
    phone VARCHAR NOT NULL                          -- номер телефона
);

CREATE TABLE car_shop.color (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL                           -- название цвета
);

CREATE TABLE car_shop.sale (
    id SERIAL PRIMARY KEY,
    person_id INT REFERENCES car_shop.person(id),
    model_id INT REFERENCES car_shop.model(id),
    car_year INT NOT NULL,                          -- год выпуска машины
    price NUMERIC(9,2) NOT NULL,                    -- цена без скидки
    discount NUMERIC(5,2),                          -- скидка в процентах
    gasoline_consumption NUMERIC(4,1),              -- может быть NULL
    sale_date DATE NOT NULL                         -- дата продажи
);

CREATE TABLE car_shop.car_color (
    sale_id INT REFERENCES car_shop.sale(id),
    color_id INT REFERENCES car_shop.color(id),
    PRIMARY KEY (sale_id, color_id)                 -- связь многие-ко-многим
);

-- --------------------------
-- Заполнение нормализованных таблиц
-- --------------------------

-- 1. brand
INSERT INTO car_shop.brand (name, origin)
SELECT DISTINCT car_brand, brand_origin FROM raw_data.sales;

-- 2. model
INSERT INTO car_shop.model (brand_id, name)
SELECT DISTINCT b.id, s.car_model
FROM raw_data.sales s
JOIN car_shop.brand b ON s.car_brand = b.name;

-- 3. person
INSERT INTO car_shop.person (name, phone)
SELECT DISTINCT person_name, phone_number FROM raw_data.sales;

-- 4. color
WITH split_colors AS (
    SELECT DISTINCT TRIM(UNNEST(STRING_TO_ARRAY(car_color, ','))) AS color
    FROM raw_data.sales
)
INSERT INTO car_shop.color (name)
SELECT DISTINCT color FROM split_colors;

-- 5. sale
INSERT INTO car_shop.sale (
    person_id, model_id, car_year, price, discount, gasoline_consumption, sale_date
)
SELECT
    p.id,
    m.id,
    s.car_year,
    s.car_price,
    s.discount,
    s.gasoline_consumption,
    s.sale_date
FROM raw_data.sales s
JOIN car_shop.person p ON s.person_name = p.name AND s.phone_number = p.phone
JOIN car_shop.brand b ON s.car_brand = b.name
JOIN car_shop.model m ON s.car_model = m.name AND m.brand_id = b.id;

-- 6. car_color
WITH sale_map AS (
    SELECT s.id AS sale_id, rd.car_color
    FROM car_shop.sale s
    JOIN raw_data.sales rd
        ON s.sale_date = rd.sale_date
        AND rd.car_model = (SELECT name FROM car_shop.model WHERE id = s.model_id)
        AND rd.person_name = (SELECT name FROM car_shop.person WHERE id = s.person_id)
),
split_colors AS (
    SELECT sale_id, TRIM(UNNEST(STRING_TO_ARRAY(car_color, ','))) AS color_name
    FROM sale_map
)
INSERT INTO car_shop.car_color (sale_id, color_id)
SELECT sc.sale_id, c.id
FROM split_colors sc
JOIN car_shop.color c ON sc.color_name = c.name;

 

-- Этап 2. Создание выборок

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.
SELECT
  ROUND(100.0 * COUNT(*) FILTER (WHERE gasoline_consumption IS NULL) / COUNT(*), 2) AS nulls_percentage_gasoline_consumption
FROM car_shop.sale;



---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки.
SELECT
  b.name AS brand_name,
  EXTRACT(YEAR FROM s.sale_date)::INT AS year,
  ROUND(AVG(s.price * (1 - s.discount / 100.0)), 2) AS price_avg
FROM car_shop.sale s
JOIN car_shop.model m ON s.model_id = m.id
JOIN car_shop.brand b ON m.brand_id = b.id
GROUP BY b.name, year
ORDER BY b.name, year;



---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки.
SELECT
  EXTRACT(MONTH FROM sale_date)::INT AS month,
  EXTRACT(YEAR FROM sale_date)::INT AS year,
  ROUND(AVG(price * (1 - discount / 100.0)), 2) AS price_avg
FROM car_shop.sale
WHERE EXTRACT(YEAR FROM sale_date) = 2022
GROUP BY month, year
ORDER BY month;



---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя.

SELECT
  p.name AS person,
  STRING_AGG(CONCAT(b.name, ' ', m.name), ', ') AS cars
FROM car_shop.sale s
JOIN car_shop.person p ON s.person_id = p.id
JOIN car_shop.model m ON s.model_id = m.id
JOIN car_shop.brand b ON m.brand_id = b.id
GROUP BY p.name
ORDER BY p.name;

---- Задание 5. Напишите запрос, который покажет количество всех пользователей из США.

SELECT
  b.origin AS brand_origin,
  MAX(s.price) AS price_max,
  MIN(s.price) AS price_min
FROM car_shop.sale s
JOIN car_shop.model m ON s.model_id = m.id
JOIN car_shop.brand b ON m.brand_id = b.id
GROUP BY b.origin
ORDER BY b.origin;
SELECT
  COUNT(*) AS persons_from_usa_count
FROM car_shop.person
WHERE phone LIKE '+1%';


