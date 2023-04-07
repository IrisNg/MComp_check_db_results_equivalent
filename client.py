import psycopg2

# To setup:
# Step 1: Activate virtual env, e.g. by running 'venv\Scripts\Activate'
# Step 2: Run 'pip install -r requirements.txt' to install psycopg

# Step 3: Install Postgresql locally on your machine, create a database and user, then configure the db settings below
db_config = {
    "dbname": "Fabian",
    "user": "postgres",
    "host": "localhost",
    "password": "1234"
}

table_name = "serialisability_1"

# Step 4: Run this python file, check table is successfully created in your database with rows seeded automatically




# def setup_db():
#     conn = get_conn()
#     cur = conn.cursor()
#     schema_file = get_file("schema.sql")
#     seed_file = get_file("seed.sql")

#     try:
#         cur.execute(schema_file)
#         cur.execute(seed_file)
#     except psycopg2.errors.DuplicateTable:
#         print('Experiment table already created, skipping...')
#     except Exception as e:
#         print('Error creating Experiment table', e)

#     conn.commit()
#     cur.close()
#     conn.close()


def get_conn():
    return psycopg2.connect(**db_config)


def get_file(filename):
    f = open(filename, "r")
    file = f.read()
    f.close()

    return file

def find_original():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT per.empid, per.lname FROM employee per, payroll pay WHERE per.empid = pay.empid AND pay.salary = 189170 ORDER BY per.empid, per.lname")
    results = cur.fetchall()
    end_results = []
    for x in results:
        end_results.append(x)
        print(x)
    cur.close()
    conn.close()
    return end_results

def q_2a():
    conn = get_conn()
    cur = conn.cursor()
    print('2a')
    cur.execute("SELECT per.empid, per.lname FROM employee per RIGHT OUTER JOIN payroll pay ON per.empid = pay.empid AND pay.salary = 189170 WHERE per.empid IS NOT NULL ORDER BY per.empid, per.lname;")
    results = cur.fetchall()
    end_results = []
    for x in results:
        end_results.append(x)
        print(x)
    cur.close()
    conn.close()
    return end_results

def q_2b():
    conn = get_conn()
    cur = conn.cursor()
    print('2b')
    cur.execute("SELECT per.empid, per.lname FROM employee per, (SELECT * FROM payroll pay) AS temp WHERE per.empid = temp.empid AND temp.salary = 189170 ORDER BY per.empid, per.lname")
    results = cur.fetchall()
    end_results = []
    for x in results:
        end_results.append(x)
        print(x)
    cur.close()
    conn.close()
    return end_results

def q_2c():
    conn = get_conn()
    cur = conn.cursor()
    print('2c')
    cur.execute("SELECT per.empid, per.lname FROM employee per WHERE NOT EXISTS (SELECT 1 FROM (SELECT COUNT(*) AS count FROM payroll pay WHERE per.empid = pay.empid AND pay.salary = 189170) AS t WHERE (CASE WHEN t.count = 0 THEN TRUE ELSE FALSE END)) ORDER BY per.empid, per.lname")
    results = cur.fetchall()
    end_results = []
    for x in results:
        end_results.append(x)
        print(x)
    cur.close()
    conn.close()
    return end_results

def q_3():
    conn = get_conn()
    cur = conn.cursor()
    print('3')
    # cur.execute("""SELECT per.empid, per.lname FROM employee per WHERE NOT EXISTS (SELECT 1 FROM (
	# SELECT COUNT(*) AS count FROM payroll outer_pay LEFT JOIN (SELECT * FROM payroll pay WHERE pay.empid != per.empid OR pay.salary != 189170 ORDER BY pay.salary, pay.empid) AS non_match ON outer_pay.empid = non_match.empid WHERE non_match.empid IS NULL
    # ) AS matches WHERE (CASE WHEN matches.count = 0 THEN TRUE ELSE FALSE END)) ORDER BY per.empid, per.lname;""")
    cur.execute('SELECT per.empid, per.lname FROM employee per WHERE per.empid IN (SELECT pay.empid FROM payroll pay WHERE per.empid = pay.empid) ORDER BY per.empid, per.lname')
    results = cur.fetchall()
    end_results = []
    for x in results:
        end_results.append(x)
        print(x)
    cur.close()
    conn.close()
    return end_results

def check_query_equivalent(ref, other, msg ):
    print(msg)
    difference = []
    try:

        for index, one_ref in enumerate(ref):
            if one_ref != other[index]:
                difference.append(one_ref)
        for index, one_other in enumerate(other):
            if one_other != ref[index]:
                difference.append(one_other)

    except Exception as e:
        print('ERROR')
        
    print('difference', difference)

    if len(difference) > 0:
        print('NOT EQUIVALENT')
    else:
        print('EQUIVALENT')

def main():
    ref = find_original()
    a_2a = q_2a()
    check_query_equivalent(ref, a_2a, 'checking 2a')
    a_2b = q_2b()
    check_query_equivalent(ref, a_2b, 'checking 2b')
    a_2c = q_2c()
    check_query_equivalent(ref, a_2c, 'checking 2c')
    a_3 = q_3()
    check_query_equivalent(ref, a_3, 'checking 3')

if __name__ == '__main__':
    main()