#!/usr/bin/env python3

import psycopg2
from datetime import datetime

DBNAME = "news"

db = psycopg2.connect(database=DBNAME)
c = db.cursor()
c.execute("""select title, path, count(*) as views
                from log, articles
                where articles.slug = split_part(log.path, '/', 3)
                group by path, title
                order by views desc
                limit 3;""")
result = c.fetchall()

print("What are the most popular three articles of all time?")
for row in result:
    print("\""+str(row[0])+"\"" + " -- " + str(row[2])+" views")
print("")


c.execute("""select articles.author, authors.name, COUNT(*) as views
                from articles
                JOIN authors
                on articles.author = authors.id
                JOIN log on articles.slug = split_part(log.path, '/', 3)
                group by articles.author, authors.name
                ORDER BY views
                DESC limit 4;""")
result = c.fetchall()
print("Who are the most popular article authors of all time?")
for row in result:
    print(str(row[1]) + " -- " + str(row[2])+" views")
print("")


c.execute("""select a.r_date,
(b.errors::DECIMAL/a.requests::DECIMAL)*100 as errors_p
from (select time::date as r_date, count(*) as requests
    from log group by r_date)as a
JOIN
(select time::date as r_date, count(*) as errors
    from log where status not like '2%' group by r_date)as b
on a.r_date = b.r_date
where (b.errors::DECIMAL/a.requests::DECIMAL)*100> 1 ;""")
result = c.fetchall()
db.close()
print("On which days did more than 1% of requests lead to errors?")
for row in result:
    d = datetime.strptime(str(row[0]), '%Y-%m-%d')
    print(d.strftime('%B %d,%Y') + " -- " + "%.2f" % row[1])

print("")
