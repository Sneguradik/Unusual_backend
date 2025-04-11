import asyncio
import csv
import io
import os
from datetime import datetime
from dotenv import load_dotenv
from quart import Quart, request, Response
from services import (
get_unusual_deals, Filter, apply_filter_chain, DbConfig,
its_kzt_filters_config, its_usd_filters_config,
spbe_usd_filters_config, spbe_rub_filters_config,
spbe_db, its_db)
import logging

app = Quart(__name__)

@app.before_serving
async def startup():
    logging.info("Starting up...")
    its_kzt_filters_config.init("default_filters/its_kzt.json")
    its_usd_filters_config.init("default_filters/its_usd.json")
    spbe_usd_filters_config.init("default_filters/spbe_usd.json")
    spbe_rub_filters_config.init("default_filters/spbe_rub.json")

    logging.info("Loaded default filters...")

    load_dotenv()
    spbe_db_config = DbConfig()
    its_db_config = DbConfig()

    spbe_db_config.init(
        os.getenv("SPBE_DB_USER"),
        os.getenv("SPBE_DB_PASSWORD"),
        os.getenv("SPBE_DB_DATABASE"),
        os.getenv("SPBE_DB_HOST"),
        os.getenv("SPBE_DB_PORT") if os.getenv("SPBE_DB_PORT") is not None else 5432,
    )

    its_db_config.init(
        os.getenv("ITS_DB_USER"),
        os.getenv("ITS_DB_PASSWORD"),
        os.getenv("ITS_DB_DATABASE"),
        os.getenv("ITS_DB_HOST"),
        os.getenv("ITS_DB_PORT") if os.getenv("ITS_DB_PORT") is not None else 5432,
    )

    await asyncio.gather(spbe_db.connect(spbe_db_config), its_db.connect(its_db_config))

    logging.info("Connected to databases...")

@app.after_serving
async def shutdown():
    await asyncio.gather(spbe_db.close(), its_db.close())

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/default_filters', methods=['POST'])
async def get_default_filters():
    body = await request.get_json()

    logging.info(f"Default filters for {body["exchange"]} {body["currency"]}")

    if body["exchange"] == "SPBE":
        if body["currency"] == "USD":
            return [el.to_dict() for el in spbe_usd_filters_config.filters], 200
        if body["currency"] == "RUB":
            return [el.to_dict() for el in spbe_rub_filters_config.filters], 200
    elif body["exchange"] == "ITS":
        if body["currency"] == "USD":
            return [el.to_dict() for el in its_usd_filters_config.filters], 200
        if body["currency"] == "KZT":
            return [el.to_dict() for el in its_kzt_filters_config.filters], 200

    logging.error("Missmatch in currency and exchange")
    return {
      "title": "Not Found",
      "status": 404,
      "detail": "Exchange or currency not found."
    }, 404


@app.route('/filter', methods=['POST'])
async def filter_trades():
    try:

        body = await request.get_json()
        logging.info(f"Filters requested for {body["date_start"]}-{body["date_finish"]} {body['exchange']} {body['currency']}")
        filters: list[Filter] = [Filter(**el) for el in body['filters']]

        if body['exchange'] == "SPBE": db = spbe_db
        else: db = its_db

        df = await get_unusual_deals({
            "date_start": datetime.fromisoformat(body['date_start']),
            "date_finish": datetime.fromisoformat(body['date_finish']),
            "currency" : body['currency'],
        }, db)

        logging.info(f"Dataframe received for {body["date_start"]}-{body["date_finish"]} {body['exchange']} {body['currency']}")

        res_df = apply_filter_chain(df,filters)

        logging.info(f"Dataframe filtered for {body["date_start"]}-{body["date_finish"]} {body['exchange']} {body['currency']}")

        return res_df.to_json(orient='records'), 200
    except Exception as e:
        return {
          "title": "Server Error",
          "status": 500,
          "detail": str(e),
        }, 500

@app.route('/filter_csv', methods=['POST'])
async def filter_trades_csv():
    try:

        body = await request.get_json()
        logging.info(f"Filters requested for {body["date_start"]}-{body["date_finish"]} {body['exchange']} {body['currency']}")
        filters: list[Filter] = [Filter(**el) for el in body['filters']]

        if body['exchange'] == "SPBE": db = spbe_db
        else: db = its_db

        df = await get_unusual_deals({
            "date_start": datetime.fromisoformat(body['date_start']),
            "date_finish": datetime.fromisoformat(body['date_finish']),
            "currency" : body['currency'],
        }, db)

        logging.info(f"Dataframe received for {body["date_start"]}-{body["date_finish"]} {body['exchange']} {body['currency']}")

        res_df = apply_filter_chain(df,filters)

        output = io.StringIO()
        res_df.to_csv(output)
        csv_data = output.getvalue()

        logging.info(f"Dataframe filtered for {body["date_start"]}-{body["date_finish"]} {body['exchange']} {body['currency']}")

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=data.csv",
            }
        )
    except Exception as e:
        return {
          "title": "Server Error",
          "status": 500,
          "detail": str(e),
        }, 500


if __name__ == '__main__':
    app.run()
