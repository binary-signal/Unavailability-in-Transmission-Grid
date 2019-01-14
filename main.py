import argparse
import json
import logging.handlers
import pickle
import sys
from timeit import default_timer as timer

import entsoe_client


def human_time(start, end, msg=""):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("{}elapsed time {:0>2}:{:0>2}:{:05.2f}".format(msg, int(hours), int(minutes), seconds))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Unavailability in Transmission Grid'
    )
    parser.add_argument("-v", "--verbose", help="increase logs verbosity",
                        action="store_true")

    args = parser.parse_args()

    rootLogger = logging.getLogger('')
    if args.verbose:
        rootLogger.setLevel(logging.INFO)

        socketHandler = logging.handlers. \
            SocketHandler('localhost',
                          logging.handlers.DEFAULT_TCP_LOGGING_PORT)
        rootLogger.addHandler(socketHandler)

    logging.getLogger(__name__)

    logging.info("\n" * 5 + "\t" * 3 + "--" * 10 + "  Session " + "--" * 10)

    t_total_start = timer()
    entsoe = entsoe_client.API(items_per_page=100)

    """
    scrape params 
    
    Note: all params are optional except 'from_date'and 'to_date' which are 
    required. You can set optional params to 'None' and scrape data with 
    default values specified in method. To decrease scrapping time and avoid
    getting banned from the server side, set country param.
    """
    # read config file
    conf_file = "config.json"
    logging.info("reading config file")
    with open(conf_file, "r") as fp:
        config = json.load(fp)

    from_date = config["from_date"]
    to_date = config["to_date"]
    country = config["country"]
    asset_type = config["asset_type"]
    outage_status = config["outage_status"]
    outage_type = config["outage_type"]
    area_type = config["area_type"]

    try:
        # fetch data
        data = entsoe. \
            transmission_grid_unavailability(from_date, to_date,
                                             country=country,
                                             outage_type=outage_type,
                                             asset_type=asset_type,
                                             outage_status=outage_status,
                                             area_type=area_type)

        pickle.dump(data, open("data.pckl", "wb"))

        # fetch details for data
        ids = [d['detailId'] for d in data]
        details = entsoe_client.API.details_grid_unavailability_batch(entsoe,
                                                                      ids)
        pickle.dump(details, open("data_detail.pckl", "wb"))

        """
        Time series data 
        
        Fetching time series data is time consuming, due to big amount
        of data and number of api calls resulting in rate limiting or remote
        host disconnections. In order to improve speed and avoid getting black  
        listed from the server use 'days_to_fetch' or 'skip_past_data' params
        or both, this way data slicing is implemented on api calls similarly to 
        slicing a python list object, but this time you are slicing the data
        fetched from the api, Great ? YEAH !
             
            skip_past_data: fetch time series data from today's date until the 
                            end of interval, skips downloading past days data
            
            days_to_fetch: specify how many days worth of data to fetch, a day 
                        is 60 data points. e.g. days_to_fetch=2 fetches  120 
                        data points and ignores the rest time series data.
                            
        """
        ids_interval = [[row['detailId'],
                         *entsoe.parse_unavailability_interval(
                             row['unavailabilityInterval']
                         )]
                        for row in data]
        t_start = timer()

        timeseries = entsoe_client.API. \
            curve_grid_unavailability_batch(entsoe, ids_interval,
                                            days_to_fetch=2,
                                            skip_past_data=False)

        pickle.dump(timeseries, open("data_series.pckl", "wb"))

        logging.info("session completed successfully")
        human_time(t_start, timer(), "time series data ")
        human_time(t_total_start, timer(), "total ")
        print("Done")

    except KeyboardInterrupt:
        logging.info("Session terminated")
        sys.exit(0)
