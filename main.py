import argparse
import logging.handlers
import pickle
import sys

import pandas as pd

import entsoe_client

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 512)

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
        SocketHandler('localhost', logging.handlers.DEFAULT_TCP_LOGGING_PORT)
    rootLogger.addHandler(socketHandler)

    logging.getLogger(__name__)

    logging.info("\n" * 5 + "\t" * 3 + "--" * 10 + "  Session " + "--" * 10)

    entsoe = entsoe_client.API(items_per_page=100)

    """
    scrape params 
    
    Note: all params are optional except 'from_date'and 'to_date' which are 
    required. You can set optional params to 'None' and scrape data with 
    default values specified in method. To decrease scrapping time and avoid
    getting banned from the server side, set country param.
    """
    from_date = "01.01.2019"
    to_date = "05.01.2019"
    country = "DE"
    asset_type = ["AC Link", "DC Link", "Substation", "Transformer",
                  "Not specified"]
    outage_status = ["Active"]
    outage_type = ["Forced", "Planned"]

    try:
        # fetch data
        data = entsoe. \
            transmission_grid_unavailability(from_date, to_date,
                                             country=country,
                                             outage_type=outage_type,
                                             asset_type=asset_type,
                                             outage_status=outage_status)

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
        ids_interval = [[row['detailId'], *entsoe.parse_unavailability_interval(row['unavailabilityInterval'])]
                        for row in data]
        timeseries = entsoe_client.API. \
            curve_grid_unavailability_batch(entsoe, ids_interval,
                                            days_to_fetch=2,
                                            skip_past_data=False)

        pickle.dump(timeseries, open("data_series.pckl", "wb"))

        logging.info("session completed successfully")
        print("Done")
    except KeyboardInterrupt:
        logging.info("Session terminated")
        sys.exit(0)
