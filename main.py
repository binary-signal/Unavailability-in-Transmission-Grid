import argparse
import logging.handlers
import pickle
import sys
from timeit import timeit as timer

import entsoe_client

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Unavailability in Transmission Grid'
    )
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    rootLogger = logging.getLogger('')
    if args.verbose:
        rootLogger.setLevel(logging.INFO)

    socketHandler = logging.handlers.SocketHandler('localhost', logging.handlers.DEFAULT_TCP_LOGGING_PORT)
    rootLogger.addHandler(socketHandler)

    logging.getLogger(__name__)

    logging.info("------------ Session ------------")

    entsoe = entsoe_client.API(items_per_page=100)

    from_date = "01.01.2019"
    to_date = "08.01.2019"
    asset_type = ["AC Link", "DC Link", "Substation", "Transformer", "Not specified"]
    outage_status = ["Active"]
    outage_type = ["Forced", "Planned"]
    country = "DE"
    print(f"Get data from {from_date} - {to_date}")
    t_start = timer()

    """
    from_date, to_date, asset_type=None,
                                         outage_type=None, outage_status=None,
                                         country=None, area_type="BORDER_CTA")
    """
    try:
        data = entsoe.transmission_grid_unavailability(from_date, to_date, country=country, outage_status=outage_status)
        pickle.dump(data, open("data.pckl", "wb"))
        data_df = entsoe.data_table_to_df(data)

        # get details by detailId in data table
        # detail = client.details_grid_unavailability(detailId)

        # WARNING !!! next block of code is painfully slow
        # fetch a batch of details
        ids = [d['detailId'] for d in data]
        details = entsoe_client.API.details_grid_unavailability_batch(entsoe, ids)
        pickle.dump(details, open("data_detail.pckl", "wb"))

        # fetch a time series by id
        # timeseries = client.curve_grid_unavailability(detailID)

        # uncomment to make a dataframe from time series data
        # timeseries_df = EntsoeApi.curve_to_df(timeseries)

        # WARNING !!! next block of code is painfully slow
        # fetch a batch of time series
        timeseries = entsoe_client.API.curve_grid_unavailability_batch(entsoe, ids)
        pickle.dump(data, open("data_series.pckl", "wb"))
    except KeyboardInterrupt:
        sys.exit(0)
