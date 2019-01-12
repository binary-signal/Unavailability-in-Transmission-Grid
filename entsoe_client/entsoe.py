import datetime
import json
import logging
import time

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

from .exceptions import *


class API(object):
    """
    API consumer Entsoe
    """
    __base_url = "https://transparency.entsoe.eu/outage-domain/r2/unavailabilityInTransmissionGrid/"
    __endpoints = {"getDataTableData/": "POST",
                   "detail": "GET",
                   "getDetailCurve/": "POST"}

    __post_headers = {
        'Origin': 'https://transparency.entsoe.eu',
        'Content-Type': 'application/json;charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
    }

    __get_headers = {
        'Origin': 'https://transparency.entsoe.eu',
        'Connection': 'keep-alive'
    }

    asset_type = {"AC Link": "B21",
                  "DC Link": "B22",
                  "Substation": "B23",
                  "Transformer": "B24",
                  "Not specified": "UNKNOWN"}

    outage_type = {"Forced": "A54",
                   "Planned": "A53"}

    outage_status = {"Active": "A05",
                     "Cancelled": "A09",
                     "Withdrawn": "A13"}

    countries = ['AL', 'AT', 'BY', 'BE', 'BA', 'BG', 'HR', 'CZ', 'DK', 'EE', 'MK', 'FI', 'FR', 'DE', 'GR', 'HU',
                 'IE',
                 'IT', 'LV', 'LT', 'LU', 'MT', 'MD', 'ME', 'NL', 'NO', 'PL', 'PT', 'RO', 'RU', 'RS', 'SK', 'SI',
                 'ES',
                 'SE', 'CH', 'TR', 'UA', 'UK']

    __pagination = [10, 25, 50, 100]

    def __init__(self, items_per_page=100):
        if items_per_page not in self.__pagination:
            raise ValueError("item_per_page domain is (10, 25, 50, 100)")
        self.items_per_page = items_per_page
        self.__borders = self.get_borders()

    @classmethod
    def __post(cls, url, params, data):
        """
        Low Level API call
        """

        try:
            response = requests.post(url, params=params, data=data,
                                     headers=cls.__post_headers)
            response.raise_for_status()
        except requests.ConnectionError as error:
            logging.error(error)
            raise error from None
        except requests.HTTPError as error:
            try:
                error_data = json.loads(response.text)
            except ValueError:
                logging.error(error)
                raise error from None
            else:
                if "errors" in error_data:
                    logging.error("post api call bad params " + error_data["errors"][0]["message"])
                    raise EntsoeApiBadParams(error_data["errors"][0]["message"]) from None
        else:
            return json.loads(response.text)

    @classmethod
    def __get(cls, url, params):
        """
        Low Level API call
        """

        try:
            response = requests.get(url, params=params, headers=cls.__get_headers)
            response.raise_for_status()
        except (requests.HTTPError, requests.ConnectionError) as error:
            logging.error(error)
            raise error from None

        else:
            return response.text

    @classmethod
    def api_call(cls, method, params=(), data=None):
        """
        Implements an api call
        """

        if method not in cls.__endpoints:
            raise EntsoeApiUnkownMethod

        if "POST" in cls.__endpoints[method] and data is None:
            raise EntsoeApiPOSTMethodMissingData

        url = cls.__base_url + method

        if cls.__endpoints[method] is "POST":
            data = json.dumps(data)
            return cls.__post(url, params, data)
        else:
            return cls.__get(url, params)

    def transmission_grid_unavailability(self, from_date, to_date, asset_type=None,
                                         outage_type=None, outage_status=None,
                                         country=None, area_type="BORDER_CTA"):
        """
        Implements api method to get unavailability in transmission grid
        """

        if country is None:
            borders = "ALL"
        else:
            if country not in self.countries:
                raise RuntimeError(f"Country code: {country} is invalid")
            borders = self.__borders[country]
        if asset_type is None:
            asset_type = self.asset_type
        if outage_type is None:
            outage_type = self.outage_type
        if outage_status is None:
            outage_status = self.outage_status

        table_data = []

        print(borders)

        params = (
            ('name', ''),
            ('defaultValue', 'false'),
            ('viewType', 'TABLE'),
            ('areaType', area_type),
            ('atch', 'false'),
            ('dateTime.dateTime', f'{from_date} 00:00|UTC|DAY'),
            ('dateTime.endDateTime', f'{to_date} 00:00|UTC|DAY'),
            ('border.values', borders),
            ('assetType.values', [self.asset_type[param] for param in asset_type if param in self.asset_type]),
            ('outageType.values', [self.outage_type[param] for param in outage_type if param in self.outage_type]),
            ('outageStatus.values',
             [self.outage_status[param] for param in outage_status if param in self.outage_status]),
        )
        import pprint
        msg = pprint.pformat(params, indent=2)
        logging.info(msg)

        data = {"sEcho": 2,  # TODO why the fuck for is this key ?
                "iColumns": 7,
                "sColumns": "status,nature,unavailabilityInterval,inArea,outArea,newNTC,",
                "iDisplayStart": 0,
                "iDisplayLength": self.items_per_page,
                "amDataProp": [0, 1, 2, 3, 4, 5, 6]}

        have = 0  # keep track of  data
        while True:
            json_data = self.api_call("getDataTableData/", params, data)

            import pprint
            pprint.pprint(
                json_data, indent=2
            )

            data_frag = self.parse_table_data(json_data)



            print(json_data['iTotalRecords'])

            have += len(data_frag)
            data.update({'iDisplayStart': have})  # set pagination offset

            # append data
            table_data = table_data + data_frag
            try:
                progress = have / json_data['iTotalRecords']
            except ZeroDivisionError:
                progress = 0

            print(f"[1/3] Data progress {round(100 * progress, 2)}%", end="\r")
            logging.info(f"fetched data | progress {have} / {json_data['iTotalRecords']}")

            if have == json_data['iTotalRecords']:
                print("\ndata completed\n")
                logging.info("data  download completed")
                break
        return table_data

    @staticmethod
    def parse_table_data(json_data):
        """
        Parses data returned from transmission_grid_unavailability method
        """
        data = [row for row in json_data['aaData']]

        data = [{
            "status": row[0],
            "nature": row[1],
            "unavailabilityInterval": row[2].replace("&nbsp;", " "),
            "inArea": row[3],
            "outArea": row[4],
            "newNTC": BeautifulSoup(row[5], "lxml").text,
            "detailId": row[6]
        } for row in data]

        for row in data:
            """
            Decode Outage Status 
            -----------------
                A05: Active
                A09: Cancelled
                A13: Withdrawn
            """
            if "A05" in row["status"]:
                row["status"] = "Active"
            elif "A09" in row["status"]:
                row["status"] = "Cancelled"
            elif "A13" in row["status"]:
                row["status"] = "Withdrawn"

            """
            Decode Outage Type 
            -----------------
                A54: Forced
                A53: Planned
            """
            if "A53" in row['nature']:
                row['nature'] = "Planned"
            elif "A54" in row["nature"]:
                row['nature'] = "Forced"
        return data

    @staticmethod
    def data_table_to_df(data):
        """
        Returns a pandas dataframe from  unavailability data in transmission grid
        """
        for d in data:
            interval = d['unavailabilityInterval']
            start_date, end_date = API.parse_unavailability_interval(interval)
            d.pop('unavailabilityInterval', None)
            d.update({"intervalStart": start_date,
                      "intervalEnd": end_date})

        return pd.DataFrame(data)

    @staticmethod
    def parse_unavailability_interval(interval):
        """
        Parses date interval in data_table_to_df function
        """
        date_string, tz = tuple(interval.rsplit(" ("))
        tz = tz.replace(")", "").strip()

        start_date, end_date = tuple(date_string.split(" - "))

        start_date = start_date.strip()
        end_date = end_date.strip()

        start_date = datetime.datetime.strptime(start_date, "%d.%m.%Y %H:%M")
        end_date = datetime.datetime.strptime(end_date, "%d.%m.%Y %H:%M")

        start_date = start_date.replace(tzinfo=pytz.timezone(tz))
        end_date = end_date.replace(tzinfo=pytz.timezone(tz))

        return start_date, end_date

    @staticmethod
    def details_to_df(data):
        """
        Returns a pandas dataframe with details data indexed on detailId
        """
        raise NotImplementedError

    def details_grid_unavailability(self, detail_id):
        """
        Implements api method to get details on unavailability in transmission
        grid
        """
        params = (
            ('detailId', detail_id),
            ('fullDetailId', detail_id),
            ('_', self.__unix_timestamp_mill()),
        )
        html_tables = self.api_call("detail", params=params)
        soup = BeautifulSoup(html_tables, 'lxml')

        details_data = []
        tables = soup.find_all("table")
        for t_id, table in enumerate(tables):
            table_rows = table.find_all("tr")

            for r_id, tr in enumerate(table_rows):
                row = []
                td = tr.find_all("td")

                for elem in td:
                    if elem.get("class"):
                        """
                        Decode Asset Types
                        -----------------
                            B21 : AC Link
                            B22 : DC Link
                            B23 : Substation
                            B24 : Transformer
                            UNKNOWN: Not specified
                        """

                        if elem.get("class")[0] in "B21":
                            row.append("AC Link")
                        elif elem.get("class")[0] in "B22":
                            row.append("DC Link")
                        elif elem.get("class")[0] in "B23":
                            row.append("Substation")
                        elif elem.get("class")[0] in "B23":
                            row.append("Transformer")
                        elif elem.get("class")[0] in "UNKNOWN":
                            row.append("Not specified")
                    else:
                        row.append(elem.text.strip())

                if row:
                    details_data += row

        if details_data.count("Failure") == 2:
            details_data.remove("Failure")
        # Fixme BUG with missing data row
        if len(details_data) != 6:
            # hack to fill in in missing values in Affected Assets when there are No Affected Assets
            logging.warning("Row id {} has missing data, fill in missing values".format(detail_id))
            for i in range(6 - len(details_data)):
                details_data.append(details_data[-1])
        details_data.append(detail_id)  # add id to each detail table for easy indexing
        return details_data

    @staticmethod
    def parse_data_details(tables_data):
        """
        Parses data returned from details_grid_unavailability method
        """
        if len(tables_data) != 7:
            raise EntsoeApiExcetpion(f"invalid  size for details : {tables_data}")
        return {"comments": tables_data[0],
                "reason": tables_data[1],
                "code": tables_data[2],
                "type": tables_data[3],
                "name": tables_data[4],
                "location": tables_data[5],
                "detailId": tables_data[6]
                }

    def curve_grid_unavailability(self, detail_id):
        """
        Implements api method getDetailCurve
        """
        timeseries_data = []
        have = page = 0

        params = (
            ('detailId', detail_id),
        )

        data = {"sEcho": 1,
                "iColumns": 2,
                "sColumns": "mtu,ntc",
                "iDisplayStart": 0,
                "iDisplayLength": self.items_per_page,
                "amDataProp": [0, 1]}

        while True:
            json_curve = self.api_call("getDetailCurve/", params, data)
            curve_frag = json_curve['aaData']

            have += len(curve_frag)
            page += 1

            timeseries_data = timeseries_data + curve_frag
            data.update({"iDisplayStart": have})
            logging.info(
                f"fetched timeseries {detail_id}  page {page} | progress have {have} / {json_curve['iTotalRecords']}")
            if have == json_curve['iTotalRecords']:
                break
        return timeseries_data

    @staticmethod
    def curve_to_df(data):
        """
        Returns a pandas dataframe from time series data indexed on date
        """
        data = [[*row[0].split(" - "), row[1]] for row in data]

        start_date = data[0][0]
        start_date = datetime.datetime.strptime(start_date, "%d.%m.%Y %H:%M")
        rng = pd.date_range(start_date, periods=len(data), freq='H')

        df = pd.DataFrame([row[2] for row in data], index=rng, columns=["ntc"])
        df.index.name = 'date'

        return df

    @staticmethod
    def __unix_timestamp_mill():
        return '{:.10f}'.format(time.time() * 1000).split('.')[0]

    @staticmethod
    def details_grid_unavailability_batch(api, detail_id_list):
        total = len(detail_id_list)
        have = 0
        detail_data = []
        for i in detail_id_list:
            try:
                detail = api.details_grid_unavailability(i)
            except Exception as error:
                logging.error(error)
                raise error from None
            else:
                detail_data.append(detail)
                have += 1
                print(f"[2/3] Details table progress {round(100 * (have / total), 2)}%", end="\r")
                logging.info(f"fetched details for {i} | progress {have} / {total}")
                time.sleep(1)

    print("\nDetails download completed\n")
    logging.info("Details download completed")

    @staticmethod
    def curve_grid_unavailability_batch(api, detail_id_list):
        total = len(detail_id_list)
        have = 0
        detail_data = []

        for i in detail_id_list:
            print(f"[3/3] Timeseries progress {round(100 * (have / total), 2)}%", end="\r")
            logging.info(f"Timeseries progress {i} | progress {have} / {total}")
            try:
                detail = api.curve_grid_unavailability(i)
            except Exception as error:
                logging.error(error)
                raise error from None
            else:
                detail_data.append(detail)
                have += 1

        print("\nTime series download completed\n")
        logging.info("Time series download completed")

    @staticmethod
    def get_borders():
        """
        returns  country -  borders dictionary
        """

        borders = {}

        response = requests.get("https://transparency.entsoe.eu/outage-domain/r2/unavailabilityInTransmissionGrid/show")
        soup = BeautifulSoup(response.text, 'lxml')
        divs = soup.find_all("div", class_="dv-sub-filter-hierarchic-wrapper")

        for country, div in zip(API.countries, divs):
            inputs = div.find_all("input")
            country_borders = []
            for i in inputs:
                border = i.get("value")
                if 'on' in border: # ignore this string in borders
                    continue
                country_borders.append(border)
                print(border)
            borders.update({country: country_borders})

        return borders
