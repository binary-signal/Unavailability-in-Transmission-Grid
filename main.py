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


def human_time(start, end, msg=""):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print(
        "{} {:0>2}:{:0>2}:{:0>2}".format(
            msg, int(hours), int(minutes), int(seconds)
        )
    )


def start_recovery():
    logging.info("resuming session starting recovery process")
    recovery_file_path = os.path.join(out, name_format + ".csv")
    try:
        fp = open(recovery_file_path, "r")
    except FileNotFoundError:
        logging.error(f"Recovery file is missing: {recovery_file_path}")
        sys.exit(-1)
    else:
        # load file names from output dir
        files = [
            f
            for f in os.listdir(out)
            if os.path.isfile(os.path.join(out, f))
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

    random.shuffle(pending)
    return pending


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

    parser.add_argument(
        "-r",
        "--resume",
        help="resume a session after it crashed",
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

    out = advanced["data_dir"]
    time_delay = advanced["time_delay"]
    skip_details = advanced["skip_details"]
    connection = advanced["connection"]
    backoff_factor = advanced["backoff_factor"]

    try:
        os.mkdir(out)
    except FileExistsError:
        pass

    # setup logging
    rootLogger = logging.getLogger("")
    if args.verbose:
        rootLogger.setLevel(logging.INFO)

        fileHandler = logging.FileHandler(advanced["log_file"], mode="w")
        f_format = logging.Formatter(
            fmt="%(asctime)-5s " "[%(levelname)-5.5s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        fileHandler.setFormatter(f_format)
        rootLogger.addHandler(fileHandler)

    logging.getLogger(__name__)

    logging.info("\n" * 5 + "\t" * 3 + "--" * 10 + "  Session " + "--" * 10)
    t_total = timer()
    client = entsoe_client.EntsoeAPI(
        connection=connection, backoff_factor=backoff_factor, items_per_page=100
    )

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

    exit_code = 0
    try:
        if args.resume:
            ids_interval = start_recovery()
        else:
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
                logging.info("skip details download")
                # fetch details for data
                ids = [d["detailId"] for d in data]
                details = entsoe_client.EntsoeAPI.details_grid_unavailability_batch(
                    client, ids, delay=time_delay
                )

                # merge data and detail into a single data frame and output as csv
                data = [{**dat, **det} for dat, det in zip(data, details)]

            data_df = pd.DataFrame(data)
            data_df.to_csv(
                os.path.join(out, f"{name_format}.csv"), header=data_df.columns
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

        entsoe_client.EntsoeAPI.curve_grid_unavailability_batch(
            client,
            ids_interval,
            from_date,
            to_date,
            delay=time_delay,
            out_dir=out,
            name_format=name_format,
        )

    except KeyboardInterrupt:
        logging.info("session terminated by user")
        exit_code = 0
    except Exception as error:
        logging.error(error)
        exit_code = -1
    else:
        logging.info("session completed successfully")
        print("Done")
        exit_code = 0
    finally:
        human_time(t_total, timer(), "run time")
        logging.info(f"#requests {client.requests_num}")
        print(f"#requests {client.requests_num}")
        sys.exit(exit_code)
