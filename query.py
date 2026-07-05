import duckdb, logging
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


DB_PATH = Path("data/warehouse.duckdb") 


if __name__ == "__main__":
    with duckdb.connect(DB_PATH, read_only=True) as conn:

        revenue_by_channel_query = """
            SELECT c.channel_desc, 
                   SUM(s.amount_sold) AS total_sold
            FROM main.fct_sales s
            INNER JOIN main.dim_channels c ON s.channel_id = c.channel_id
            GROUP BY c.channel_desc
            ORDER BY total_sold DESC
        """
        revenue_by_channel_result = conn.execute(revenue_by_channel_query).fetchall()
        for row in revenue_by_channel_result:
            logging.info(f"Channel Description: {row[0]}, Total Sold: {row[1]}")
                         
        
        top_10_products_query = """
            SELECT 
                p.prod_name,
                SUM(s.amount_sold) AS total_sold
            FROM main.fct_sales s
            INNER JOIN main.dim_products p ON s.prod_id = p.prod_id
            GROUP BY p.prod_name
            ORDER BY total_sold DESC
            LIMIT 10
        """
        top_10_products_result = conn.execute(top_10_products_query).fetchall()
        for row in top_10_products_result:
            logging.info(f"Product Name: {row[0]}, Total Sold: {row[1]}")

        
        revenue_by_calendar_year_query = """
            SELECT
                t.calendar_year,
                SUM(s.amount_sold) AS total_sold
            FROM main.fct_sales s
            INNER JOIN main.dim_times t ON s.time_id = t.time_id
            GROUP BY t.calendar_year
            ORDER BY t.calendar_year
        """
        revenue_by_calendar_year_result = conn.execute(revenue_by_calendar_year_query).fetchall()
        for row in revenue_by_calendar_year_result:
            logging.info(f"Calendar Year: {row[0]}, Total Sold: {row[1]}")
        
        channel_rank_query = """
           WITH groupped_sales_cte AS (
                SELECT 
                    c.channel_desc,
                    SUM(s.amount_sold) AS total_sold
                FROM main.fct_sales s
                INNER JOIN main.dim_channels c ON s.channel_id = c.channel_id
                GROUP BY c.channel_desc
            )
            SELECT 
                channel_desc,
                total_sold,
                DENSE_RANK() OVER (ORDER BY total_sold DESC) AS channel_rank
            FROM groupped_sales_cte
            ORDER BY channel_rank
        """

        channel_rank_result = conn.execute(channel_rank_query).fetchall()
        for row in channel_rank_result:
            logging.info(f"Channel Description: {row[0]}, Total Sold: {row[1]}, Channel Rank: {row[2]}")