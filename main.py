import argparse
import json
import logging.handlers
import os
import sys
from timeit import default_timer as timer
import random

import pandas as pd

import entsoe_client

CONF_FILE = "config.json"


def human_time(start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:0>2}".format(
        int(hours), int(minutes), int(seconds)
    )


def start_recovery(name_format):
    logging.info("resuming session starting recovery process")
    recovery_file_path = os.path.join(data_dir, name_format + ".csv")
    try:
        fp = open(recovery_file_path, "r")
    except FileNotFoundError:
        logging.info(f"no recovery file found: {recovery_file_path}")
        return []
    else:
        # load file names from output dir
        files = [
            f
            for f in os.listdir(data_dir)
            if os.path.isfile(os.path.join(data_dir, f))
            if name_format + ".csv" not in str(f)
        ]

        ids = [f.rsplit("_")[-1].split(".")[0] for f in files]

        df = pd.read_csv(fp)

        pending = [
            [
                row["detailId"],
                row["unavailabilityStart"],
                row["unavailabilityEnd"],
            ]
            for (i, row) in df.iterrows()
            if row["detailId"] not in ids
        ]

        if len(pending) > 0:
            random.shuffle(pending)
            return pending
        elif len(pending) == 0:
            logging.info(
                "session is already completed go grab a cup of coffee !"
            )
            sys.exit(0)


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
        print("config file is missing")
        sys.exit(-1)
    except ValueError as error:
        print(
            f"config file is corrupted check " f"in config file: \n'{error}'"
        )
        sys.exit(-1)
    else:
        try:
            advanced = config["advanced"]
        except KeyError:
            advanced ={}
        session = config["session"]

    # read scrape params - session
    print(f"reading config file {CONF_FILE}")
    try:
        from_date = session["from_date"]
        to_date = session["to_date"]
        area_type = session["area_type"]
        country = session["country"]
    except KeyError as error:
        print(
            f"error while parsing {CONF_FILE}, required param '{error}' is missing in config file"
        )
        sys.exit(-1)

    asset_type = session.pop("asset_type", None)
    outage_status = session.pop("outage_status", None)
    outage_type = session.pop("outage_type", None)

    name_format = (
        f"{country}_{area_type}_{from_date.replace('.', '_')}"
        f"_{to_date.replace('.', '_')}"
    )

    # read advanced config - session
    log_file = advanced.pop("log_file", False)
    req_delay = advanced.pop("request_delay", 3)
    data_dir = advanced.pop("data_dir", "session_data")
    connection = advanced.pop("connection", 10)
    backoff_factor = advanced.pop("backoff_factor", 0.5)
    skip_details = advanced.pop("skip_details", False)
    skip_timeseries = advanced.pop("skip_timeseries", False)
    pause_req = advanced.pop("pause_after_requests", 100)
    pause_int = advanced.pop("pause_internal", 30)
    conn_rst_int = advanced.pop("connection_reset_interval", 300)

    try:
        os.mkdir(data_dir)
    except FileExistsError:
        pass

    # setup logging
    rootLogger = logging.getLogger("")
    if args.verbose:
        rootLogger.setLevel(logging.INFO)

    if log_file:
        fileHandler = logging.FileHandler(name_format + ".log", mode="w")
        fileLogFormatter = logging.Formatter(
            "%(asctime)s %(name)s [%(levelname)-5.5s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fileHandler.setFormatter(fileLogFormatter)
        rootLogger.addHandler(fileHandler)

    consoleLogFormatter = logging.Formatter("[%(levelname)-5.5s]  %(message)s")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(consoleLogFormatter)
    rootLogger.addHandler(consoleHandler)

    logging.getLogger(__name__)

    logging.info("\n" * 5 + "\t" * 3 + "--" * 10 + "  Session " + "--" * 10)
    t_total = timer()
    client = entsoe_client.EntsoeAPI(
        connection=connection,
        backoff_factor=backoff_factor,
        items_per_page=100,
        pause_req=pause_req,
        pause_int=pause_int,
        conn_rst_int=conn_rst_int,
        req_delay=req_delay,
    )

    if skip_details:
        logging.info("skip detail download")

    if skip_timeseries:
        logging.info("skip timeseries download")

    exit_code = 0
    ids_interval = start_recovery(name_format)

    try:
        # no recovery file found start from the beginning
        if len(ids_interval) is 0:
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

            if not skip_details:
                # fetch details for data
                ids = [d["detailId"] for d in data]
                details = client.details_grid_unavailability_batch(ids)

                # merge data and detail into a single data frame and output as csv
                data = [{**dat, **det} for dat, det in zip(data, details)]

            data_df = pd.DataFrame(data)
            data_df.to_csv(
                os.path.join(data_dir, f"{name_format}.csv"),
                header=data_df.columns,
            )

            # download time series data
            ids_interval = [
                [
                    row["detailId"],
                    row["unavailabilityStart"],
                    row["unavailabilityEnd"],
                ]
                for row in data
            ]

        print(f"todate {to_date}")
        if not skip_timeseries:
            client.curve_grid_unavailability_batch(
                ids_interval,
                from_date,
                to_date,
                name_format=name_format,
                out_dir=data_dir,
            )

    except KeyboardInterrupt:
        logging.info("session terminated by user")
        exit_code = 0
    except Exception as error:
        logging.error(error)
        exit_code = -1
    else:
        logging.info("session completed successfully")
        print("Done.")
        exit_code = 0
    finally:
        time = human_time(t_total, timer())
        logging.info(f"requests: {client.requests_num} | time:{time}")
        sys.exit(exit_code)
