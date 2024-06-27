import mysql.connector


def reset_tables(cursor):
    try:
        # Delete data from sales table first (which has the foreign key constraint)
        cursor.execute("DELETE FROM sales")
        
        # Delete data from products table
        cursor.execute("DELETE FROM products")

    except mysql.connector.Error as err:
        print(f"error: {err}")


#connecting to database
def find_profitable_products():
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="root",
            database="challengedb"
        )
        
        mycursor = db.cursor()
        reset_tables(mycursor)

        #creating tables
        mycursor.execute("CREATE TABLE IF NOT EXISTS products (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC)")
        mycursor.execute("CREATE TABLE IF NOT EXISTS sales (id INT PRIMARY KEY, product_id INT, quantity INTEGER, revenue NUMERIC, FOREIGN KEY (product_id) REFERENCES products(id))")

#filling tables with products    
        mycursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", (1, "product1", 2.00))
        mycursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", (2, "product2", 10.00))
        mycursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", (3, "product3", 6.00))
        mycursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", (4, "product4", 8.00))
        mycursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", (5, "product5", 15.00))
        mycursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", (6, "product6", 3.00))

        # Insert data into sales table
        mycursor.execute("INSERT INTO sales (id, product_id, quantity, revenue) VALUES (%s, %s, %s, %s)", (1, 1, 20, 400.00))
        mycursor.execute("INSERT INTO sales (id, product_id, quantity, revenue) VALUES (%s, %s, %s, %s)", (2, 2, 5, 500.00))
        mycursor.execute("INSERT INTO sales (id, product_id, quantity, revenue) VALUES (%s, %s, %s, %s)", (3, 3, 10, 600.00))
        mycursor.execute("INSERT INTO sales (id, product_id, quantity, revenue) VALUES (%s, %s, %s, %s)", (4, 4, 20, 200.00))
        mycursor.execute("INSERT INTO sales (id, product_id, quantity, revenue) VALUES (%s, %s, %s, %s)", (5, 5, 5, 60.00))
        mycursor.execute("INSERT INTO sales (id, product_id, quantity, revenue) VALUES (%s, %s, %s, %s)", (6, 6, 10, 900.00))
        
#committing changes to database.

        db.commit()
        
        profitable_products(mycursor)
    
#try/except for error handling
    except mysql.connector.Error as err:
        print(f"error: {err}")
    
    finally:
        if mycursor:
            mycursor.close()
        if db:
            db.close()

#function for calculating most profitable products
def profitable_products(mycursor):
    query = """
        SELECT 
            p.id, 
            p.name, 
            SUM(s.quantity) AS total_quantity_sold, 
            p.price AS unit_price,
            SUM(s.revenue) AS total_revenue,
            (SUM(s.revenue) - SUM(s.quantity) * p.price) AS profitability
        FROM 
            products p
        INNER JOIN 
            sales s ON p.id = s.product_id
        GROUP BY 
            p.id, p.name, p.price
        ORDER BY 
            profitability DESC
        LIMIT 3
    """
    
    mycursor.execute(query)
    
    results = mycursor.fetchall()

    #printing the top 3 most profitable

    print("top 3 profitable:")
    for row in results:
        product_id, product_name, quantity_sold, price, total_revenue, profitability = row
        print(f"product ID: {product_id}, name: {product_name}, quantity: {quantity_sold}, price: ${price:.2f}, revenue: ${total_revenue:.2f}, profitability: ${profitability:.2f}")

find_profitable_products()
