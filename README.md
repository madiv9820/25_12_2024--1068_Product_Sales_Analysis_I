# [1068. Product Sales Analysis I](https://leetcode.com/problems/product-sales-analysis-i?envType=study-plan-v2&envId=top-sql-50)
- ### Intuition
    The task requires retrieving specific details (`product_name`, `year`, and `price`) about each sale from the given `Sales` and `Product` tables. The tables are connected through the `product_id` column, which acts as a foreign key in the `Sales` table, referencing the `Product` table. This relationship allows us to combine data from both tables to get the desired output.

- ### Approach
    1. **Understand the Tables**:
        - The `Sales` table provides details about sales, including `sale_id`, `product_id`, `year`, `quantity`, and `price`.
        - The `Product` table provides details about the product, including `product_id` and `product_name`.

    2. **Relationship**:
        - The `product_id` column serves as the connecting link between the two tables (`foreign key` in `Sales`, `primary key` in `Product`).

    3. **Query Design**:
        - Use an `INNER JOIN` operation to combine the `Sales` table with the `Product` table based on the `product_id` column.
        - Select the relevant columns: `product_name` from the `Product` table and `year` and `price` from the `Sales` table.

    4. **Reason for Columns**:
        - `product_name` is fetched to identify the product sold.
        - `year` indicates when the sale occurred.
        - `price` shows the unit price of the product for that specific sale.

    5. **Output**:
        - The resulting table includes rows with the product name, sale year, and price for each sale as required.

    This approach applies universally, whether implemented in SQL, PySpark, or Pandas, as all follow the concept of joining tables and selecting specific columns for the final result.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT product_name, year, price FROM Product
        INNER JOIN Sales ON
        Product.product_id = Sales.product_id
        ```
    - **PySpark:**
        ```python3 []
        def sales_analysis(sales: pyspark.sql.dataframe.DataFrame, 
                        product: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            output = sales.join(product, on = 'product_id', how = 'inner')\
                            .select(['product_name', 'year', 'price'])
            return output
        ```
    - **Pandas:**
        ```python3 []
        def sales_analysis(sales: pd.DataFrame, product: pd.DataFrame) -> pd.DataFrame:
            output = sales.merge(product, on = 'product_id', how = 'inner')
            output = output[['product_name', 'year', 'price']]
            return output
        ```