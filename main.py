import argparse
import json
import logging.handlers
import os
import sys
from timeit import default_timer as timer

import pandas as pd

import entsoe_client


CONF_FILE = "config.json"


def human_time(start, end, msg=""):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print(
        "{} {:0>2}:{:0>2}:{:0>2}".format(
            msg, int(hours), int(minutes), int(seconds)
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Unavailability in Transmission Grid"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase logs verbosity, "
        "output log to file or to "
        "external console",
        action="store_true",
    )

    args = parser.parse_args()

    # read config file
    try:
        with open(CONF_FILE, "r") as fp:
            config = json.load(fp)
    except OSError:
        raise RuntimeError("config file is missing")
    except ValueError as error:
        raise RuntimeError(
            f"config file is corrupted check " f"in config file: \n'{error}'"
        )
    else:
        advanced = config["advanced"]
        session = config["session"]

    try:
        os.mkdir(advanced["data_dir"])
    except FileExistsError:
        pass

    out = advanced["data_dir"]

    # setup logging
    rootLogger = logging.getLogger("")
    if args.verbose:
        rootLogger.setLevel(logging.INFO)

        fileHandler = logging.FileHandler(advanced["log_file"], mode='w')
        f_format = logging.Formatter(
            fmt="%(asctime)-5s " "[%(levelname)-5.5s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        fileHandler.setFormatter(f_format)
        rootLogger.addHandler(fileHandler)

    logging.getLogger(__name__)

    logging.info("\n" * 5 + "\t" * 3 + "--" * 10 + "  Session " + "--" * 10)

    t_total = timer()
    client = entsoe_client.EntsoeAPI(items_per_page=100)

    """
    scrape params 
    
    Note: To decrease scrapping time and avoid
    getting banned from the server side, set country param.
    """

    try:
        from_date = session["from_date"]
        to_date = session["to_date"]
        area_type = session["area_type"]
    except KeyError as error:
        sys.exit(f"required param '{error}' is missing in config file")

    country = session.pop("country", None)
    asset_type = session.pop("asset_type", None)
    outage_status = session.pop("outage_status", None)
    outage_type = session.pop("outage_type", None)

    name_format = (
        f"{country}_{area_type}_{from_date.replace('.', '_')}"
        f"_{to_date.replace('.', '_')}"
    )
    try:
        # fetch data
        data = client.transmission_grid_unavailability(
            from_date=from_date,
            to_date=to_date,
            area_type=area_type,
            country=country,
            outage_type=outage_type,
            asset_type=asset_type,
            outage_status=outage_status,
        )

        # fetch details for data
        ids = [d["detailId"] for d in data]
        details = entsoe_client.EntsoeAPI.details_grid_unavailability_batch(
            client, ids
        )

        # merge data and detail into a single data frame and output as csv
        combined_data = [{**dat, **det} for dat, det in zip(data, details)]
        data_df = pd.DataFrame(combined_data)

        data_df.to_csv(
            os.path.join(out, f"{name_format}.csv"), header=data_df.columns
        )
        json.dump(
            {"data": combined_data}, open(os.path.join(out, "data.json"), "w")
        )

        """
        Time series data    
        """
        ids_interval = [
            [
                row["detailId"],
                row["unavailabilityStart"],
                row["unavailabilityEnd"],
            ]
            for row in data
        ]

        t_series = timer()
        timeseries = entsoe_client.EntsoeAPI.curve_grid_unavailability_batch(
            client, ids_interval, from_date, to_date
        )

        json.dump(
            {"timeseries": timeseries},
            open(os.path.join(out, "timeseries.json"), "w"),
        )

        ids = [list(ts.keys())[0] for ts in timeseries]
        timeseries = [ts[list(ts.keys())[0]] for ts in timeseries]
        df_list = [client.curve_to_df(ts) for ts in timeseries]

        for f_name, df in zip(ids, df_list):
            df.to_csv(
                os.path.join(out, f"{name_format}_{f_name}.csv"),
                header=df.columns,
            )

        logging.info("session completed successfully")
        human_time(t_series, timer(), "series ")
        human_time(t_total, timer(), "total  ")
        print("Done")

    except KeyboardInterrupt:
        logging.info("Session terminated")
    finally:
        sys.exit(0)
