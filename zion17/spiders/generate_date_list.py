from omegaconf import OmegaConf
import psycopg2 as pg

conf = OmegaConf.load('conf/server/db/postgres.yaml')

# conn = pg.connect(
#     dbname=conf.postgres.dbname,
#     user=conf.postgres.user,
#     password=conf.postgres.password,
#     host=conf.postgres.host,
#     port=conf.postgres.port
# )

def change_date_format(date):
    parts = date.split('-')
    new_date = parts[2] + '-' + parts[1] + '-' + parts[0]
    return new_date


def check_date_to_db_banks(from_dt, to_dt):

    conn = pg.connect(
    dbname=conf.postgres.dbname,
    user=conf.postgres.user,
    password=conf.postgres.password,
    host=conf.postgres.host,
    port=conf.postgres.port
    )

    query = f"""
            with SBQ as 
            (
                select 	distinct date_page::date
                from 	myfin.myfin_raw cp 
            )
            select 	gen_date::date::text as search_dt
            from	generate_series(%s, %s, interval '1 day') as gen_date
            where 	gen_date::date not in (select date_page from SBQ)
            """
    cursor = conn.cursor()
    cursor.execute(query, (from_dt, to_dt))
    search_dt = [change_date_format(row[0]) for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    return search_dt


def check_date_to_db_nbrb(from_dt, to_dt):

    conn = pg.connect(
    dbname=conf.postgres.dbname,
    user=conf.postgres.user,
    password=conf.postgres.password,
    host=conf.postgres.host,
    port=conf.postgres.port
    )

    query = f"""
            with SBQ as 
            (
                select 	distinct date_page::date
                from 	myfin.myfin_raw cp 
                where 	myfin_bank_id = 999999
            )
            select 	gen_date::date::text as search_dt
            from	generate_series(%s, %s, interval '1 day') as gen_date
            where 	gen_date::date not in (select date_page from SBQ)
            """
    cursor = conn.cursor()
    cursor.execute(query, (from_dt, to_dt))
    search_dt = [change_date_format(row[0]) for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    return search_dt

if __name__ == "__main__":
    pass
