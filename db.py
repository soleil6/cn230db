import sqlite3

conn = sqlite3.connect('game_consoles.db')
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS Generation (
    generation_id INTEGER PRIMARY KEY,
    generation_name TEXT NOT NULL
);
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS TimePeriod (
    time_period_id INTEGER PRIMARY KEY,
    generation_id INTEGER NOT NULL,
    time_period TEXT NOT NULL,
    FOREIGN KEY (generation_id) REFERENCES Generation(generation_id)
);
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Console (
    console_id INTEGER PRIMARY KEY,
    generation_id INTEGER NOT NULL,
    time_period_id INTEGER NOT NULL,
    console_name TEXT NOT NULL,
    year_of_release INTEGER NOT NULL,
    game_media TEXT NOT NULL,
    FOREIGN KEY (generation_id) REFERENCES Generation(generation_id),
    FOREIGN KEY (time_period_id) REFERENCES TimePeriod(time_period_id)
);
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Price (
    price_id INTEGER PRIMARY KEY,
    console_id INTEGER NOT NULL,
    original_price TEXT NOT NULL,
    adjusted_price_2022 TEXT NOT NULL,
    FOREIGN KEY (console_id) REFERENCES Console(console_id)
);
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Sales (
    sales_id INTEGER PRIMARY KEY,
    console_id INTEGER NOT NULL,
    total_systems_sold TEXT NOT NULL,
    FOREIGN KEY (console_id) REFERENCES Console(console_id)
);
''')

generation_data = [
    (1, 'First'),
    (2, 'Second')
]
cur.executemany('INSERT OR IGNORE INTO Generation (generation_id, generation_name) VALUES (?, ?)', generation_data)


time_period_data = [
    (1, 1, '1972-1978'),
    (2, 1, '1972-1979'),
    (3, 2, '1976-1984')
]
cur.executemany('INSERT OR IGNORE INTO TimePeriod (time_period_id, generation_id, time_period) VALUES (?, ?, ?)', time_period_data)


console_data = [
    (1, 1, 1, 'Magnavox Odyssey', 1972, 'Dedicated'),
    (2, 1, 2, 'Home Pong', 1977, 'Dedicated'),
    (3, 2, 3, 'Fairchild Channel F', 1976, 'Game cartridges')
]
cur.executemany('INSERT OR IGNORE INTO Console (console_id, generation_id, time_period_id, console_name, year_of_release, game_media) VALUES (?, ?, ?, ?, ?, ?)', console_data)


price_data = [
    (1, 1, '$99.00', '$611.21'),
    (2, 2, '$98.95', '$504.07'),
    (3, 3, '$169.95', '$910.92')
]
cur.executemany('INSERT OR IGNORE INTO Price (price_id, console_id, original_price, adjusted_price_2022) VALUES (?, ?, ?, ?)', price_data)


sales_data = [
    (1, 1, '350,000'),
    (2, 2, '150,000'),
    (3, 3, '250,000')
]
cur.executemany('INSERT OR IGNORE INTO Sales (sales_id, console_id, total_systems_sold) VALUES (?, ?, ?)', sales_data)

conn.commit()



print("\n1. Estimated revenue per console:\n")
cur.execute("""
SELECT c.console_name,
       p.original_price,
       s.total_systems_sold,
       ROUND(CAST(REPLACE(REPLACE(p.original_price, '$', ''), ',', '') AS FLOAT) *
             CAST(REPLACE(REPLACE(s.total_systems_sold, ',', ''), ' ', '') AS FLOAT), 2) AS estimated_revenue
FROM Console c
JOIN Price p ON c.console_id = p.console_id
JOIN Sales s ON c.console_id = s.console_id
ORDER BY estimated_revenue DESC
""")
for row in cur.fetchall():
    print(f"{row[0]:30} | Revenue: ${row[3]:,.2f}")


print("\n2. Inflation ratio (Adjusted / Original):\n")
cur.execute("""
SELECT c.console_name,
       p.original_price,
       p.adjusted_price_2022,
       ROUND(
           CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT) /
           CAST(REPLACE(p.original_price, '$', '') AS FLOAT), 2
       ) AS inflation_ratio
FROM Console c
JOIN Price p ON c.console_id = p.console_id
ORDER BY inflation_ratio DESC
""")
for row in cur.fetchall():
    print(f"{row[0]:30} | Ratio: {row[3]}x")

print("\n3. Adjusted price summary per generation:\n")
cur.execute("""
SELECT g.generation_name,
       ROUND(AVG(CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT)), 2) AS avg_price,
       MAX(CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT)) AS max_price,
       MIN(CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT)) AS min_price
FROM Console c
JOIN Price p ON c.console_id = p.console_id
JOIN Generation g ON c.generation_id = g.generation_id
GROUP BY g.generation_name
ORDER BY avg_price DESC
""")
for row in cur.fetchall():
    print(f"{row[0]:20} | Avg: ${row[1]} | Max: ${row[2]} | Min: ${row[3]}")

print("\n4. Dominant time period per generation:\n")
cur.execute("""
SELECT generation_name, time_period
FROM (
    SELECT g.generation_name, t.time_period, COUNT(*) AS count,
           RANK() OVER (PARTITION BY g.generation_name ORDER BY COUNT(*) DESC) AS rank
    FROM Console c
    JOIN TimePeriod t ON c.time_period_id = t.time_period_id
    JOIN Generation g ON c.generation_id = g.generation_id
    GROUP BY g.generation_name, t.time_period
)
WHERE rank = 1
""")
for row in cur.fetchall():
    print(f"{row[0]:20} | Most consoles released in: {row[1]}")

print("\n5. Console value efficiency (Units sold per adjusted dollar):\n")
cur.execute("""
SELECT c.console_name,
       s.total_systems_sold,
       p.adjusted_price_2022,
       ROUND(
           CAST(REPLACE(s.total_systems_sold, ',', '') AS FLOAT) /
           CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT), 2
       ) AS efficiency
FROM Console c
JOIN Sales s ON c.console_id = s.console_id
JOIN Price p ON c.console_id = p.console_id
ORDER BY efficiency DESC
LIMIT 5
""")
for row in cur.fetchall():
    print(f"{row[0]:30} | Efficiency: {row[3]:,.2f} units per $")



print("\n6. Price anomalies compared to generation average:\n")
cur.execute("""
WITH GenAvg AS (
    SELECT g.generation_id,
           ROUND(AVG(CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT)), 2) AS avg_gen_price
    FROM Generation g
    JOIN Console c ON g.generation_id = c.generation_id
    JOIN Price p ON c.console_id = p.console_id
    GROUP BY g.generation_id
)
SELECT c.console_name,
       g.generation_name,
       p.adjusted_price_2022,
       GenAvg.avg_gen_price,
       ROUND(
           CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT) - GenAvg.avg_gen_price, 2
       ) AS price_diff
FROM Console c
JOIN Price p ON c.console_id = p.console_id
JOIN Generation g ON c.generation_id = g.generation_id
JOIN GenAvg ON GenAvg.generation_id = g.generation_id
ORDER BY price_diff DESC;
""")

rows = cur.fetchall()
for name, gen, price, avg, diff in rows:
    print(f"{name:25} | Gen: {gen:15}\n | Price: ${price} | Avg: ${avg} | Δ ${diff}")

print("\n7. Media evolution across generations:\n")
cur.execute("""
WITH DominantMedia AS (
    SELECT g.generation_name,
           c.game_media,
           COUNT(*) AS count
    FROM Generation g
    JOIN Console c ON g.generation_id = c.generation_id
    GROUP BY g.generation_name, c.game_media
),
RankedMedia AS (
    SELECT generation_name, game_media,
           RANK() OVER (PARTITION BY generation_name ORDER BY count DESC) as rnk
    FROM DominantMedia
)
SELECT generation_name, game_media
FROM RankedMedia
WHERE rnk = 1;
""")

rows = cur.fetchall()
for gen, media in rows:
    print(f"{gen:25} | Dominant Media: {media}")


print("\n8. Most underrated consoles (Low Price, High Sales):\n")
cur.execute("""
SELECT c.console_name,
       p.adjusted_price_2022,
       s.total_systems_sold,
       ROUND(
           CAST(REPLACE(REPLACE(s.total_systems_sold, ',', ''), 'M', '') AS FLOAT) /
           CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT), 2
       ) AS efficiency_ratio
FROM Console c
JOIN Price p ON c.console_id = p.console_id
JOIN Sales s ON c.console_id = s.console_id
ORDER BY efficiency_ratio DESC
LIMIT 5;
""")

rows = cur.fetchall()
for name, price, sold, ratio in rows:
    print(f"{name:30} | Price: {price}\n | Sold: {sold} | Ratio: {ratio:,.2f} units/$")

print("\n9. Estimated revenue per generation (in millions):\n")
cur.execute("""
SELECT g.generation_name,
       ROUND(SUM(
           CAST(REPLACE(s.total_systems_sold, 'M', '') AS FLOAT) *
           CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT)
       ), 2) AS estimated_revenue
FROM Generation g
JOIN Console c ON g.generation_id = c.generation_id
JOIN Price p ON c.console_id = p.console_id
JOIN Sales s ON c.console_id = s.console_id
GROUP BY g.generation_name
ORDER BY estimated_revenue DESC;
""")

for gen, rev in cur.fetchall():
    print(f"{gen:25} | Estimated Revenue: ${rev:.2f}M")


print("\n10.Expensive flops: Expensive consoles with low sales:\n")
cur.execute("""
SELECT c.console_name,
       p.adjusted_price_2022,
       s.total_systems_sold,
       ROUND(
           CAST(REPLACE(s.total_systems_sold, ',', '') AS FLOAT) /
           CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT), 2
       ) AS value_ratio
FROM Console c
JOIN Price p ON c.console_id = p.console_id
JOIN Sales s ON c.console_id = s.console_id
WHERE CAST(REPLACE(p.adjusted_price_2022, '$', '') AS FLOAT) > 400
  AND CAST(REPLACE(s.total_systems_sold, ',', '') AS FLOAT) < 15000000
ORDER BY value_ratio ASC;
""")

rows = cur.fetchall()
for name, price, sold, ratio in rows:
    print(f"{name:30} | Price: {price}\n | Sold: {sold} | Value Ratio: {ratio:.2f}")

print("\n11. Estimated lifespan of each generation (Years):\n")
cur.execute("""
SELECT g.generation_name,
       MIN(c.year_of_release) AS start_year,
       MAX(c.year_of_release) AS end_year,
       MAX(c.year_of_release) - MIN(c.year_of_release) + 1 AS lifespan
FROM Generation g
JOIN Console c ON g.generation_id = c.generation_id
GROUP BY g.generation_name
ORDER BY lifespan DESC;
""")

for gen, start, end, span in cur.fetchall():
    print(f"{gen:25} | From {start} to {end} ({span} years)")


conn.close()
