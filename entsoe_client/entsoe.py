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
    API consumer for Entsoe
    """
    __base_url = "https://transparency.entsoe.eu/outage-domain/r2/" \
                 "unavailabilityInTransmissionGrid/"
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

    countries = ['AL', 'AT', 'BY', 'BE', 'BA', 'BG', 'HR', 'CZ', 'DK', 'EE',
                 'MK', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT',
                 'LU', 'MT', 'MD', 'ME', 'NL', 'NO', 'PL', 'PT', 'RO', 'RU',
                 'RS', 'SK', 'SI', 'ES', 'SE', 'CH', 'TR', 'UA', 'UK']

    __pagination = [10, 25, 50, 100]

    def __init__(self, items_per_page=100):
        if items_per_page not in self.__pagination:
            raise ValueError("item_per_page domain is (10, 25, 50, 100)")
        self.items_per_page = items_per_page
        self.__cta_borders = {'AL': ['CTY|10YAL-KESH-----5!CTA_CTA|10YAL-KESH-----5_CTA_CTA|10YGR-HTSO-----Y',
                                     'CTY|10YAL-KESH-----5!CTA_CTA|10YAL-KESH-----5_CTA_CTA|10YCS-CG-TSO---S',
                                     'CTY|10YAL-KESH-----5!CTA_CTA|10YAL-KESH-----5_CTA_CTA|10YCS-SERBIATSOV'],
                              'AT': ['CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YCH-SWISSGRIDZ',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YCZ-CEPS-----N',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YDE-RWENET---I',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YDE-EON------1',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YDE-ENBW-----N',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YHU-MAVIR----U',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YIT-GRTN-----B',
                                     'CTY|10YAT-APG------L!CTA_CTA|10YAT-APG------L_CTA_CTA|10YSI-ELES-----O'],
                              'BA': ['CTY|10YBA-JPCC-----D!CTA_CTA|10YBA-JPCC-----D_CTA_CTA|10YHR-HEP------M',
                                     'CTY|10YBA-JPCC-----D!CTA_CTA|10YBA-JPCC-----D_CTA_CTA|10YCS-CG-TSO---S',
                                     'CTY|10YBA-JPCC-----D!CTA_CTA|10YBA-JPCC-----D_CTA_CTA|10YCS-SERBIATSOV'],
                              'BE': ['CTY|10YBE----------2!CTA_CTA|10YBE----------2_CTA_CTA|10YFR-RTE------C',
                                     'CTY|10YBE----------2!CTA_CTA|10YBE----------2_CTA_CTA|10YGB----------A',
                                     'CTY|10YBE----------2!CTA_CTA|10YBE----------2_CTA_CTA|10YLU-CEGEDEL-NQ',
                                     'CTY|10YBE----------2!CTA_CTA|10YBE----------2_CTA_CTA|10YNL----------L'],
                              'BG': ['CTY|10YCA-BULGARIA-R!CTA_CTA|10YCA-BULGARIA-R_CTA_CTA|10YGR-HTSO-----Y',
                                     'CTY|10YCA-BULGARIA-R!CTA_CTA|10YCA-BULGARIA-R_CTA_CTA|10YMK-MEPSO----8',
                                     'CTY|10YCA-BULGARIA-R!CTA_CTA|10YCA-BULGARIA-R_CTA_CTA|10YRO-TEL------P',
                                     'CTY|10YCA-BULGARIA-R!CTA_CTA|10YCA-BULGARIA-R_CTA_CTA|10YCS-SERBIATSOV',
                                     'CTY|10YCA-BULGARIA-R!CTA_CTA|10YCA-BULGARIA-R_CTA_CTA|10YTR-TEIAS----W'],
                              'BY': ['CTY|BY!CTA_CTA|10Y1001A1001A51S_CTA_CTA|10YLT-1001A0008Q',
                                     'CTY|BY!CTA_CTA|10Y1001A1001A51S_CTA_CTA|10Y1001C--000182'],
                              'CH': ['CTY|10YCH-SWISSGRIDZ!CTA_CTA|10YCH-SWISSGRIDZ_CTA_CTA|10YAT-APG------L',
                                     'CTY|10YCH-SWISSGRIDZ!CTA_CTA|10YCH-SWISSGRIDZ_CTA_CTA|10YDE-RWENET---I',
                                     'CTY|10YCH-SWISSGRIDZ!CTA_CTA|10YCH-SWISSGRIDZ_CTA_CTA|10YDE-ENBW-----N',
                                     'CTY|10YCH-SWISSGRIDZ!CTA_CTA|10YCH-SWISSGRIDZ_CTA_CTA|10YFR-RTE------C',
                                     'CTY|10YCH-SWISSGRIDZ!CTA_CTA|10YCH-SWISSGRIDZ_CTA_CTA|10YIT-GRTN-----B'],
                              'CZ': ['CTY|10YCZ-CEPS-----N!CTA_CTA|10YCZ-CEPS-----N_CTA_CTA|10YAT-APG------L',
                                     'CTY|10YCZ-CEPS-----N!CTA_CTA|10YCZ-CEPS-----N_CTA_CTA|10YDE-VE-------2',
                                     'CTY|10YCZ-CEPS-----N!CTA_CTA|10YCZ-CEPS-----N_CTA_CTA|10YDE-EON------1',
                                     'CTY|10YCZ-CEPS-----N!CTA_CTA|10YCZ-CEPS-----N_CTA_CTA|10YPL-AREA-----S',
                                     'CTY|10YCZ-CEPS-----N!CTA_CTA|10YCZ-CEPS-----N_CTA_CTA|10YSK-SEPS-----K',
                                     'CTY|10YCZ-CEPS-----N!CTA_CTA|10YDOM-1001A082L_CTA_CTA|10YDE-VE-------2'],
                              'DE': ['CTY|10Y1001A1001A83F!CTA_CTA|10YDE-VE-------2_CTA_CTA|10YCZ-CEPS-----N',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-VE-------2_CTA_CTA|10Y1001A1001A796',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-VE-------2_CTA_CTA|10YPL-AREA-----S',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-VE-------2_CTA_CTA|10YDOM-1001A082L',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-RWENET---I_CTA_CTA|10YAT-APG------L',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-RWENET---I_CTA_CTA|10YCH-SWISSGRIDZ',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-RWENET---I_CTA_CTA|10YFR-RTE------C',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-RWENET---I_CTA_CTA|10YLU-CEGEDEL-NQ',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-RWENET---I_CTA_CTA|10YNL----------L',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-EON------1_CTA_CTA|10YAT-APG------L',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-EON------1_CTA_CTA|10YCZ-CEPS-----N',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-EON------1_CTA_CTA|10Y1001A1001A796',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-EON------1_CTA_CTA|10YNL----------L',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-EON------1_CTA_CTA|10YSE-1--------K',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-ENBW-----N_CTA_CTA|10YAT-APG------L',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-ENBW-----N_CTA_CTA|10YCH-SWISSGRIDZ',
                                     'CTY|10Y1001A1001A83F!CTA_CTA|10YDE-ENBW-----N_CTA_CTA|10YFR-RTE------C'],
                              'DK': ['CTY|10Y1001A1001A65H!CTA_CTA|10Y1001A1001A796_CTA_CTA|10YDE-VE-------2',
                                     'CTY|10Y1001A1001A65H!CTA_CTA|10Y1001A1001A796_CTA_CTA|10YDE-EON------1',
                                     'CTY|10Y1001A1001A65H!CTA_CTA|10Y1001A1001A796_CTA_CTA|10YNO-0--------C',
                                     'CTY|10Y1001A1001A65H!CTA_CTA|10Y1001A1001A796_CTA_CTA|10YSE-1--------K'],
                              'EE': ['CTY|10Y1001A1001A39I!CTA_CTA|10Y1001A1001A39I_CTA_CTA|10YFI-1--------U',
                                     'CTY|10Y1001A1001A39I!CTA_CTA|10Y1001A1001A39I_CTA_CTA|10YLV-1001A00074',
                                     'CTY|10Y1001A1001A39I!CTA_CTA|10Y1001A1001A39I_CTA_CTA|10Y1001A1001A49F'],
                              'ES': ['CTY|10YES-REE------0!CTA_CTA|10YES-REE------0_CTA_CTA|10YFR-RTE------C',
                                     'CTY|10YES-REE------0!CTA_CTA|10YES-REE------0_CTA_CTA|10YPT-REN------W'],
                              'FI': ['CTY|10YFI-1--------U!CTA_CTA|10YFI-1--------U_CTA_CTA|10Y1001A1001A39I',
                                     'CTY|10YFI-1--------U!CTA_CTA|10YFI-1--------U_CTA_CTA|10YNO-0--------C',
                                     'CTY|10YFI-1--------U!CTA_CTA|10YFI-1--------U_CTA_CTA|10Y1001A1001A49F',
                                     'CTY|10YFI-1--------U!CTA_CTA|10YFI-1--------U_CTA_CTA|10YSE-1--------K'],
                              'FR': ['CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YBE----------2',
                                     'CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YCH-SWISSGRIDZ',
                                     'CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YDE-RWENET---I',
                                     'CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YDE-ENBW-----N',
                                     'CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YES-REE------0',
                                     'CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YIT-GRTN-----B',
                                     'CTY|10YFR-RTE------C!CTA_CTA|10YFR-RTE------C_CTA_CTA|10YGB----------A'],
                              'GR': ['CTY|10YGR-HTSO-----Y!CTA_CTA|10YGR-HTSO-----Y_CTA_CTA|10YAL-KESH-----5',
                                     'CTY|10YGR-HTSO-----Y!CTA_CTA|10YGR-HTSO-----Y_CTA_CTA|10YCA-BULGARIA-R',
                                     'CTY|10YGR-HTSO-----Y!CTA_CTA|10YGR-HTSO-----Y_CTA_CTA|10YIT-GRTN-----B',
                                     'CTY|10YGR-HTSO-----Y!CTA_CTA|10YGR-HTSO-----Y_CTA_CTA|10YMK-MEPSO----8',
                                     'CTY|10YGR-HTSO-----Y!CTA_CTA|10YGR-HTSO-----Y_CTA_CTA|10YTR-TEIAS----W'],
                              'HR': ['CTY|10YHR-HEP------M!CTA_CTA|10YHR-HEP------M_CTA_CTA|10YBA-JPCC-----D',
                                     'CTY|10YHR-HEP------M!CTA_CTA|10YHR-HEP------M_CTA_CTA|10YHU-MAVIR----U',
                                     'CTY|10YHR-HEP------M!CTA_CTA|10YHR-HEP------M_CTA_CTA|10YCS-SERBIATSOV',
                                     'CTY|10YHR-HEP------M!CTA_CTA|10YHR-HEP------M_CTA_CTA|10YSI-ELES-----O'],
                              'HU': ['CTY|10YHU-MAVIR----U!CTA_CTA|10YHU-MAVIR----U_CTA_CTA|10YAT-APG------L',
                                     'CTY|10YHU-MAVIR----U!CTA_CTA|10YHU-MAVIR----U_CTA_CTA|10YHR-HEP------M',
                                     'CTY|10YHU-MAVIR----U!CTA_CTA|10YHU-MAVIR----U_CTA_CTA|10YRO-TEL------P',
                                     'CTY|10YHU-MAVIR----U!CTA_CTA|10YHU-MAVIR----U_CTA_CTA|10YCS-SERBIATSOV',
                                     'CTY|10YHU-MAVIR----U!CTA_CTA|10YHU-MAVIR----U_CTA_CTA|10YSK-SEPS-----K',
                                     'CTY|10YHU-MAVIR----U!CTA_CTA|10YHU-MAVIR----U_CTA_CTA|10YUA-WEPS-----0'],
                              'IE': ['CTY|10YIE-1001A00010!CTA_CTA|10YIE-1001A00010_CTA_CTA|10Y1001A1001A016',
                                     'CTY|10YIE-1001A00010!CTA_CTA|10YIE-1001A00010_CTA_CTA|10YGB----------A'],
                              'IT': ['CTY|10YIT-GRTN-----B!CTA_CTA|10YIT-GRTN-----B_CTA_CTA|10YAT-APG------L',
                                     'CTY|10YIT-GRTN-----B!CTA_CTA|10YIT-GRTN-----B_CTA_CTA|10YCH-SWISSGRIDZ',
                                     'CTY|10YIT-GRTN-----B!CTA_CTA|10YIT-GRTN-----B_CTA_CTA|10YFR-RTE------C',
                                     'CTY|10YIT-GRTN-----B!CTA_CTA|10YIT-GRTN-----B_CTA_CTA|10YGR-HTSO-----Y',
                                     'CTY|10YIT-GRTN-----B!CTA_CTA|10YIT-GRTN-----B_CTA_CTA|10Y1001A1001A93C',
                                     'CTY|10YIT-GRTN-----B!CTA_CTA|10YIT-GRTN-----B_CTA_CTA|10YSI-ELES-----O'],
                              'LT': ['CTY|10YLT-1001A0008Q!CTA_CTA|10YLT-1001A0008Q_CTA_CTA|10Y1001A1001A51S',
                                     'CTY|10YLT-1001A0008Q!CTA_CTA|10YLT-1001A0008Q_CTA_CTA|10YLV-1001A00074',
                                     'CTY|10YLT-1001A0008Q!CTA_CTA|10YLT-1001A0008Q_CTA_CTA|10YPL-AREA-----S',
                                     'CTY|10YLT-1001A0008Q!CTA_CTA|10YLT-1001A0008Q_CTA_CTA|10Y1001A1001A50U',
                                     'CTY|10YLT-1001A0008Q!CTA_CTA|10YLT-1001A0008Q_CTA_CTA|10YSE-1--------K'],
                              'LU': ['CTY|10YLU-CEGEDEL-NQ!CTA_CTA|10YLU-CEGEDEL-NQ_CTA_CTA|10YBE----------2',
                                     'CTY|10YLU-CEGEDEL-NQ!CTA_CTA|10YLU-CEGEDEL-NQ_CTA_CTA|10YDE-RWENET---I'],
                              'LV': ['CTY|10YLV-1001A00074!CTA_CTA|10YLV-1001A00074_CTA_CTA|10Y1001A1001A39I',
                                     'CTY|10YLV-1001A00074!CTA_CTA|10YLV-1001A00074_CTA_CTA|10YLT-1001A0008Q',
                                     'CTY|10YLV-1001A00074!CTA_CTA|10YLV-1001A00074_CTA_CTA|10Y1001A1001A49F'],
                              'MD': ['CTY|10Y1001A1001A990!CTA_CTA|10Y1001A1001A990_CTA_CTA|10YRO-TEL------P',
                                     'CTY|10Y1001A1001A990!CTA_CTA|10Y1001A1001A990_CTA_CTA|10Y1001C--000182'],
                              'ME': ['CTY|10YCS-CG-TSO---S!CTA_CTA|10YCS-CG-TSO---S_CTA_CTA|10YAL-KESH-----5',
                                     'CTY|10YCS-CG-TSO---S!CTA_CTA|10YCS-CG-TSO---S_CTA_CTA|10YBA-JPCC-----D',
                                     'CTY|10YCS-CG-TSO---S!CTA_CTA|10YCS-CG-TSO---S_CTA_CTA|10YCS-SERBIATSOV'],
                              'MK': ['CTY|10YMK-MEPSO----8!CTA_CTA|10YMK-MEPSO----8_CTA_CTA|10YCA-BULGARIA-R',
                                     'CTY|10YMK-MEPSO----8!CTA_CTA|10YMK-MEPSO----8_CTA_CTA|10YGR-HTSO-----Y',
                                     'CTY|10YMK-MEPSO----8!CTA_CTA|10YMK-MEPSO----8_CTA_CTA|10YCS-SERBIATSOV'],
                              'MT': ['CTY|10Y1001A1001A93C!CTA_CTA|10Y1001A1001A93C_CTA_CTA|10YIT-GRTN-----B'],
                              'NL': ['CTY|10YNL----------L!CTA_CTA|10YNL----------L_CTA_CTA|10YBE----------2',
                                     'CTY|10YNL----------L!CTA_CTA|10YNL----------L_CTA_CTA|10YDE-RWENET---I',
                                     'CTY|10YNL----------L!CTA_CTA|10YNL----------L_CTA_CTA|10YDE-EON------1',
                                     'CTY|10YNL----------L!CTA_CTA|10YNL----------L_CTA_CTA|10YNO-0--------C',
                                     'CTY|10YNL----------L!CTA_CTA|10YNL----------L_CTA_CTA|10YGB----------A'],
                              'NO': ['CTY|10YNO-0--------C!CTA_CTA|10YNO-0--------C_CTA_CTA|10Y1001A1001A796',
                                     'CTY|10YNO-0--------C!CTA_CTA|10YNO-0--------C_CTA_CTA|10YFI-1--------U',
                                     'CTY|10YNO-0--------C!CTA_CTA|10YNO-0--------C_CTA_CTA|10YNL----------L',
                                     'CTY|10YNO-0--------C!CTA_CTA|10YNO-0--------C_CTA_CTA|10YSE-1--------K'],
                              'PL': ['CTY|10YPL-AREA-----S!CTA_CTA|10YPL-AREA-----S_CTA_CTA|10YCZ-CEPS-----N',
                                     'CTY|10YPL-AREA-----S!CTA_CTA|10YPL-AREA-----S_CTA_CTA|10YDE-VE-------2',
                                     'CTY|10YPL-AREA-----S!CTA_CTA|10YPL-AREA-----S_CTA_CTA|10YLT-1001A0008Q',
                                     'CTY|10YPL-AREA-----S!CTA_CTA|10YPL-AREA-----S_CTA_CTA|10YSE-1--------K',
                                     'CTY|10YPL-AREA-----S!CTA_CTA|10YPL-AREA-----S_CTA_CTA|10YSK-SEPS-----K',
                                     'CTY|10YPL-AREA-----S!CTA_CTA|10YPL-AREA-----S_CTA_CTA|10Y1001A1001A869'],
                              'PT': ['CTY|10YPT-REN------W!CTA_CTA|10YPT-REN------W_CTA_CTA|10YES-REE------0'],
                              'RO': ['CTY|10YRO-TEL------P!CTA_CTA|10YRO-TEL------P_CTA_CTA|10YCA-BULGARIA-R',
                                     'CTY|10YRO-TEL------P!CTA_CTA|10YRO-TEL------P_CTA_CTA|10YHU-MAVIR----U',
                                     'CTY|10YRO-TEL------P!CTA_CTA|10YRO-TEL------P_CTA_CTA|10Y1001A1001A990',
                                     'CTY|10YRO-TEL------P!CTA_CTA|10YRO-TEL------P_CTA_CTA|10YCS-SERBIATSOV',
                                     'CTY|10YRO-TEL------P!CTA_CTA|10YRO-TEL------P_CTA_CTA|10YUA-WEPS-----0'],
                              'RS': ['CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YAL-KESH-----5',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YBA-JPCC-----D',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YCA-BULGARIA-R',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YHR-HEP------M',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YHU-MAVIR----U',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YCS-CG-TSO---S',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YMK-MEPSO----8',
                                     'CTY|10YCS-SERBIATSOV!CTA_CTA|10YCS-SERBIATSOV_CTA_CTA|10YRO-TEL------P'],
                              'RU': ['CTY|RU!CTA_CTA|10Y1001A1001A49F_CTA_CTA|10Y1001A1001A39I',
                                     'CTY|RU!CTA_CTA|10Y1001A1001A49F_CTA_CTA|10YFI-1--------U',
                                     'CTY|RU!CTA_CTA|10Y1001A1001A49F_CTA_CTA|10YLV-1001A00074',
                                     'CTY|RU!CTA_CTA|10Y1001A1001A49F_CTA_CTA|10Y1001C--000182',
                                     'CTY|RU!CTA_CTA|10Y1001A1001A50U_CTA_CTA|10YLT-1001A0008Q'],
                              'SE': ['CTY|10YSE-1--------K!CTA_CTA|10YSE-1--------K_CTA_CTA|10YDE-EON------1',
                                     'CTY|10YSE-1--------K!CTA_CTA|10YSE-1--------K_CTA_CTA|10Y1001A1001A796',
                                     'CTY|10YSE-1--------K!CTA_CTA|10YSE-1--------K_CTA_CTA|10YFI-1--------U',
                                     'CTY|10YSE-1--------K!CTA_CTA|10YSE-1--------K_CTA_CTA|10YLT-1001A0008Q',
                                     'CTY|10YSE-1--------K!CTA_CTA|10YSE-1--------K_CTA_CTA|10YNO-0--------C',
                                     'CTY|10YSE-1--------K!CTA_CTA|10YSE-1--------K_CTA_CTA|10YPL-AREA-----S'],
                              'SI': ['CTY|10YSI-ELES-----O!CTA_CTA|10YSI-ELES-----O_CTA_CTA|10YAT-APG------L',
                                     'CTY|10YSI-ELES-----O!CTA_CTA|10YSI-ELES-----O_CTA_CTA|10YHR-HEP------M',
                                     'CTY|10YSI-ELES-----O!CTA_CTA|10YSI-ELES-----O_CTA_CTA|10YIT-GRTN-----B'],
                              'SK': ['CTY|10YSK-SEPS-----K!CTA_CTA|10YSK-SEPS-----K_CTA_CTA|10YCZ-CEPS-----N',
                                     'CTY|10YSK-SEPS-----K!CTA_CTA|10YSK-SEPS-----K_CTA_CTA|10YHU-MAVIR----U',
                                     'CTY|10YSK-SEPS-----K!CTA_CTA|10YSK-SEPS-----K_CTA_CTA|10YPL-AREA-----S',
                                     'CTY|10YSK-SEPS-----K!CTA_CTA|10YSK-SEPS-----K_CTA_CTA|10YUA-WEPS-----0'],
                              'TR': ['CTY|10YTR-TEIAS----W!CTA_CTA|10YTR-TEIAS----W_CTA_CTA|10YCA-BULGARIA-R',
                                     'CTY|10YTR-TEIAS----W!CTA_CTA|10YTR-TEIAS----W_CTA_CTA|10YGR-HTSO-----Y'],
                              'UA': ['CTY|10Y1001C--00003F!CTA_CTA|10YUA-WEPS-----0_CTA_CTA|10YHU-MAVIR----U',
                                     'CTY|10Y1001C--00003F!CTA_CTA|10YUA-WEPS-----0_CTA_CTA|10YRO-TEL------P',
                                     'CTY|10Y1001C--00003F!CTA_CTA|10YUA-WEPS-----0_CTA_CTA|10YSK-SEPS-----K',
                                     'CTY|10Y1001C--00003F!CTA_CTA|10Y1001A1001A869_CTA_CTA|10YPL-AREA-----S',
                                     'CTY|10Y1001C--00003F!CTA_CTA|10Y1001C--000182_CTA_CTA|10Y1001A1001A51S',
                                     'CTY|10Y1001C--00003F!CTA_CTA|10Y1001C--000182_CTA_CTA|10Y1001A1001A990',
                                     'CTY|10Y1001C--00003F!CTA_CTA|10Y1001C--000182_CTA_CTA|10Y1001A1001A49F'],
                              'UK': ['CTY|GB!CTA_CTA|10YGB----------A_CTA_CTA|10YBE----------2',
                                     'CTY|GB!CTA_CTA|10Y1001A1001A016_CTA_CTA|10YIE-1001A00010',
                                     'CTY|GB!CTA_CTA|10Y1001A1001A016_CTA_CTA|10YGB----------A',
                                     'CTY|GB!CTA_CTA|10YGB----------A_CTA_CTA|10YFR-RTE------C',
                                     'CTY|GB!CTA_CTA|10YGB----------A_CTA_CTA|10YIE-1001A00010',
                                     'CTY|GB!CTA_CTA|10YGB----------A_CTA_CTA|10YNL----------L']}

        self.__bzn_borders = {'AL': ['CTY|10YAL-KESH-----5!BZN_BZN|10YAL-KESH-----5_BZN_BZN|10YGR-HTSO-----Y',
                                     'CTY|10YAL-KESH-----5!BZN_BZN|10YAL-KESH-----5_BZN_BZN|10YCS-CG-TSO---S',
                                     'CTY|10YAL-KESH-----5!BZN_BZN|10YAL-KESH-----5_BZN_BZN|10YCS-SERBIATSOV'],
                              'AT': ['CTY|10YAT-APG------L!BZN_BZN|10YAT-APG------L_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YAT-APG------L!BZN_BZN|10YAT-APG------L_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10YAT-APG------L!BZN_BZN|10YAT-APG------L_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YAT-APG------L!BZN_BZN|10YAT-APG------L_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YAT-APG------L!BZN_BZN|10YAT-APG------L_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YAT-APG------L!BZN_BZN|10YAT-APG------L_BZN_BZN|10YSI-ELES-----O',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YBE----------2',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YDK-1--------W',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YDK-2--------M',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A80L',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YNL----------L',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A47J',
                                     'CTY|10YAT-APG------L!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YSI-ELES-----O'],
                              'BA': ['CTY|10YBA-JPCC-----D!BZN_BZN|10YBA-JPCC-----D_BZN_BZN|10YHR-HEP------M',
                                     'CTY|10YBA-JPCC-----D!BZN_BZN|10YBA-JPCC-----D_BZN_BZN|10YCS-CG-TSO---S',
                                     'CTY|10YBA-JPCC-----D!BZN_BZN|10YBA-JPCC-----D_BZN_BZN|10YCS-SERBIATSOV'],
                              'BE': ['CTY|10YBE----------2!BZN_BZN|10YBE----------2_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YBE----------2!BZN_BZN|10YBE----------2_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YBE----------2!BZN_BZN|10YBE----------2_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YBE----------2!BZN_BZN|10YBE----------2_BZN_BZN|10YGB----------A',
                                     'CTY|10YBE----------2!BZN_BZN|10YBE----------2_BZN_BZN|10YNL----------L'],
                              'BG': ['CTY|10YCA-BULGARIA-R!BZN_BZN|10YCA-BULGARIA-R_BZN_BZN|10YGR-HTSO-----Y',
                                     'CTY|10YCA-BULGARIA-R!BZN_BZN|10YCA-BULGARIA-R_BZN_BZN|10YMK-MEPSO----8',
                                     'CTY|10YCA-BULGARIA-R!BZN_BZN|10YCA-BULGARIA-R_BZN_BZN|10YRO-TEL------P',
                                     'CTY|10YCA-BULGARIA-R!BZN_BZN|10YCA-BULGARIA-R_BZN_BZN|10YCS-SERBIATSOV',
                                     'CTY|10YCA-BULGARIA-R!BZN_BZN|10YCA-BULGARIA-R_BZN_BZN|10YTR-TEIAS----W'],
                              'BY': ['CTY|BY!BZN_BZN|10Y1001A1001A51S_BZN_BZN|10YLT-1001A0008Q',
                                     'CTY|BY!BZN_BZN|10Y1001A1001A51S_BZN_BZN|10Y1001C--00003F'],
                              'CH': ['CTY|10YCH-SWISSGRIDZ!BZN_BZN|10YCH-SWISSGRIDZ_BZN_BZN|10YAT-APG------L',
                                     'CTY|10YCH-SWISSGRIDZ!BZN_BZN|10YCH-SWISSGRIDZ_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YCH-SWISSGRIDZ!BZN_BZN|10YCH-SWISSGRIDZ_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YCH-SWISSGRIDZ!BZN_BZN|10YCH-SWISSGRIDZ_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YCH-SWISSGRIDZ!BZN_BZN|10YCH-SWISSGRIDZ_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YCH-SWISSGRIDZ!BZN_BZN|10YCH-SWISSGRIDZ_BZN_BZN|10Y1001A1001A68B'],
                              'CZ': ['CTY|10YCZ-CEPS-----N!BZN_BZN|10YCZ-CEPS-----N_BZN_BZN|10YAT-APG------L',
                                     'CTY|10YCZ-CEPS-----N!BZN_BZN|10YCZ-CEPS-----N_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YCZ-CEPS-----N!BZN_BZN|10YCZ-CEPS-----N_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YCZ-CEPS-----N!BZN_BZN|10YCZ-CEPS-----N_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YCZ-CEPS-----N!BZN_BZN|10YCZ-CEPS-----N_BZN_BZN|10YSK-SEPS-----K',
                                     'CTY|10YCZ-CEPS-----N!BZN_BZN|10YDOM-CZ-DE-SKK_BZN_BZN|10YPL-AREA-----S'],
                              'DE': ['CTY|10Y1001A1001A83F!BZN_BZN|10YDOM-CZ-DE-SKK_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YBE----------2',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YDK-1--------W',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YDK-2--------M',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A80L',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YNL----------L',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A47J',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YSI-ELES-----O',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YAT-APG------L',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YBE----------2',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YDK-1--------W',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YDK-2--------M',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YNL----------L',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10Y1001A1001A83F!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10Y1001A1001A47J'],
                              'DK': ['CTY|10Y1001A1001A65H!BZN_BZN|10YDK-1--------W_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-1--------W_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-1--------W_BZN_BZN|10YDK-2--------M',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-1--------W_BZN_BZN|10YNO-2--------T',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-1--------W_BZN_BZN|10Y1001A1001A46L',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-2--------M_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-2--------M_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10Y1001A1001A65H!BZN_BZN|10YDK-2--------M_BZN_BZN|10Y1001A1001A47J'],
                              'EE': ['CTY|10Y1001A1001A39I!BZN_BZN|10Y1001A1001A39I_BZN_BZN|10YFI-1--------U',
                                     'CTY|10Y1001A1001A39I!BZN_BZN|10Y1001A1001A39I_BZN_BZN|10YLV-1001A00074',
                                     'CTY|10Y1001A1001A39I!BZN_BZN|10Y1001A1001A39I_BZN_BZN|10Y1001A1001A49F'],
                              'ES': ['CTY|10YES-REE------0!BZN_BZN|10YES-REE------0_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YES-REE------0!BZN_BZN|10YES-REE------0_BZN_BZN|10YPT-REN------W'],
                              'FI': ['CTY|10YFI-1--------U!BZN_BZN|10YFI-1--------U_BZN_BZN|10Y1001A1001A39I',
                                     'CTY|10YFI-1--------U!BZN_BZN|10YFI-1--------U_BZN_BZN|10YNO-4--------9',
                                     'CTY|10YFI-1--------U!BZN_BZN|10YFI-1--------U_BZN_BZN|10Y1001A1001A49F',
                                     'CTY|10YFI-1--------U!BZN_BZN|10YFI-1--------U_BZN_BZN|10Y1001A1001A44P',
                                     'CTY|10YFI-1--------U!BZN_BZN|10YFI-1--------U_BZN_BZN|10Y1001A1001A46L'],
                              'FR': ['CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10YBE----------2',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10YES-REE------0',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10YGB----------A',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YFR-RTE------C!BZN_BZN|10YFR-RTE------C_BZN_BZN|10Y1001A1001A81J'],
                              'GR': ['CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10YAL-KESH-----5',
                                     'CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10YCA-BULGARIA-R',
                                     'CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10Y1001A1001A788',
                                     'CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10Y1001A1001A699',
                                     'CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10Y1001A1001A66F',
                                     'CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10YMK-MEPSO----8',
                                     'CTY|10YGR-HTSO-----Y!BZN_BZN|10YGR-HTSO-----Y_BZN_BZN|10YTR-TEIAS----W'],
                              'HR': ['CTY|10YHR-HEP------M!BZN_BZN|10YHR-HEP------M_BZN_BZN|10YBA-JPCC-----D',
                                     'CTY|10YHR-HEP------M!BZN_BZN|10YHR-HEP------M_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YHR-HEP------M!BZN_BZN|10YHR-HEP------M_BZN_BZN|10YCS-SERBIATSOV',
                                     'CTY|10YHR-HEP------M!BZN_BZN|10YHR-HEP------M_BZN_BZN|10YSI-ELES-----O'],
                              'HU': ['CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10YAT-APG------L',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10YHR-HEP------M',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10YRO-TEL------P',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10YCS-SERBIATSOV',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10YSK-SEPS-----K',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10Y1001C--00003F',
                                     'CTY|10YHU-MAVIR----U!BZN_BZN|10YHU-MAVIR----U_BZN_BZN|10YUA-WEPS-----0'],
                              'IE': ['CTY|10YIE-1001A00010!BZN_BZN|10Y1001A1001A59C_BZN_BZN|10YGB----------A'],
                              'IT': ['CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A73I_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A73I_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A73I_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A73I_BZN_BZN|10YSI-ELES-----O',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A788_BZN_BZN|10YGR-HTSO-----Y',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A699_BZN_BZN|10YGR-HTSO-----Y',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A699_BZN_BZN|10Y1001A1001A788',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A70O_BZN_BZN|10Y1001A1001A71M',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A70O_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A70O_BZN_BZN|10Y1001A1001A893',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A71M_BZN_BZN|10Y1001A1001A74G',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A71M_BZN_BZN|10Y1001A1001A788',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A72K_BZN_BZN|10Y1001A1001A788',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A66F_BZN_BZN|10YGR-HTSO-----Y',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A877_BZN_BZN|10Y1001A1001A93C',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A73I_BZN_BZN|10YAT-APG------L',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A80L_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A68B_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A81J_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A67D_BZN_BZN|10YSI-ELES-----O',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A76C_BZN_BZN|10Y1001A1001A75E',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A77A_BZN_BZN|10Y1001A1001A75E',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A77A_BZN_BZN|10Y1001A1001A788',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A885_BZN_BZN|10Y1001A1001A74G',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A893_BZN_BZN|10Y1001A1001A74G',
                                     'CTY|10YIT-GRTN-----B!BZN_BZN|10Y1001A1001A75E_BZN_BZN|10Y1001A1001A93C'],
                              'LT': ['CTY|10YLT-1001A0008Q!BZN_BZN|10YLT-1001A0008Q_BZN_BZN|10Y1001A1001A51S',
                                     'CTY|10YLT-1001A0008Q!BZN_BZN|10YLT-1001A0008Q_BZN_BZN|10YLV-1001A00074',
                                     'CTY|10YLT-1001A0008Q!BZN_BZN|10YLT-1001A0008Q_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YLT-1001A0008Q!BZN_BZN|10YLT-1001A0008Q_BZN_BZN|10Y1001A1001A50U',
                                     'CTY|10YLT-1001A0008Q!BZN_BZN|10YLT-1001A0008Q_BZN_BZN|10Y1001A1001A47J'],
                              'LU': ['CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YBE----------2',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YDK-1--------W',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YDK-2--------M',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A80L',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YNL----------L',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10Y1001A1001A47J',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A63L_BZN_BZN|10YSI-ELES-----O',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YAT-APG------L',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YBE----------2',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YCH-SWISSGRIDZ',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YDK-1--------W',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YDK-2--------M',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YFR-RTE------C',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YNL----------L',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YLU-CEGEDEL-NQ!BZN_BZN|10Y1001A1001A82H_BZN_BZN|10Y1001A1001A47J'],
                              'LV': ['CTY|10YLV-1001A00074!BZN_BZN|10YLV-1001A00074_BZN_BZN|10Y1001A1001A39I',
                                     'CTY|10YLV-1001A00074!BZN_BZN|10YLV-1001A00074_BZN_BZN|10YLT-1001A0008Q',
                                     'CTY|10YLV-1001A00074!BZN_BZN|10YLV-1001A00074_BZN_BZN|10Y1001A1001A49F'],
                              'MD': ['CTY|10Y1001A1001A990!BZN_BZN|10Y1001A1001A990_BZN_BZN|10YRO-TEL------P',
                                     'CTY|10Y1001A1001A990!BZN_BZN|10Y1001A1001A990_BZN_BZN|10Y1001C--00003F'],
                              'ME': ['CTY|10YCS-CG-TSO---S!BZN_BZN|10YCS-CG-TSO---S_BZN_BZN|10YAL-KESH-----5',
                                     'CTY|10YCS-CG-TSO---S!BZN_BZN|10YCS-CG-TSO---S_BZN_BZN|10YBA-JPCC-----D',
                                     'CTY|10YCS-CG-TSO---S!BZN_BZN|10YCS-CG-TSO---S_BZN_BZN|10YCS-SERBIATSOV'],
                              'MK': ['CTY|10YMK-MEPSO----8!BZN_BZN|10YMK-MEPSO----8_BZN_BZN|10YCA-BULGARIA-R',
                                     'CTY|10YMK-MEPSO----8!BZN_BZN|10YMK-MEPSO----8_BZN_BZN|10YGR-HTSO-----Y',
                                     'CTY|10YMK-MEPSO----8!BZN_BZN|10YMK-MEPSO----8_BZN_BZN|10YCS-SERBIATSOV'],
                              'MT': ['CTY|10Y1001A1001A93C!BZN_BZN|10Y1001A1001A93C_BZN_BZN|10Y1001A1001A877',
                                     'CTY|10Y1001A1001A93C!BZN_BZN|10Y1001A1001A93C_BZN_BZN|10Y1001A1001A75E'],
                              'NL': ['CTY|10YNL----------L!BZN_BZN|10YNL----------L_BZN_BZN|10YBE----------2',
                                     'CTY|10YNL----------L!BZN_BZN|10YNL----------L_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YNL----------L!BZN_BZN|10YNL----------L_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YNL----------L!BZN_BZN|10YNL----------L_BZN_BZN|10YGB----------A',
                                     'CTY|10YNL----------L!BZN_BZN|10YNL----------L_BZN_BZN|10YNO-2--------T'],
                              'NO': ['CTY|10YNO-0--------C!BZN_BZN|10YNO-1--------2_BZN_BZN|10YNO-2--------T',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-1--------2_BZN_BZN|10YNO-3--------J',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-1--------2_BZN_BZN|10Y1001A1001A48H',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-1--------2_BZN_BZN|10Y1001A1001A46L',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-2--------T_BZN_BZN|10YDK-1--------W',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-2--------T_BZN_BZN|10YNL----------L',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-2--------T_BZN_BZN|10Y1001A1001A48H',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-3--------J_BZN_BZN|10YNO-4--------9',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-3--------J_BZN_BZN|10Y1001A1001A48H',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-3--------J_BZN_BZN|10Y1001A1001A45N',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-4--------9_BZN_BZN|10YFI-1--------U',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-4--------9_BZN_BZN|10Y1001A1001A44P',
                                     'CTY|10YNO-0--------C!BZN_BZN|10YNO-4--------9_BZN_BZN|10Y1001A1001A45N'],
                              'PL': ['CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10YDOM-CZ-DE-SKK',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10YLT-1001A0008Q',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10Y1001A1001A47J',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10YSK-SEPS-----K',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10Y1001C--00003F',
                                     'CTY|10YPL-AREA-----S!BZN_BZN|10YPL-AREA-----S_BZN_BZN|10Y1001A1001A869'],
                              'PT': ['CTY|10YPT-REN------W!BZN_BZN|10YPT-REN------W_BZN_BZN|10YES-REE------0'],
                              'RO': ['CTY|10YRO-TEL------P!BZN_BZN|10YRO-TEL------P_BZN_BZN|10YCA-BULGARIA-R',
                                     'CTY|10YRO-TEL------P!BZN_BZN|10YRO-TEL------P_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YRO-TEL------P!BZN_BZN|10YRO-TEL------P_BZN_BZN|10Y1001A1001A990',
                                     'CTY|10YRO-TEL------P!BZN_BZN|10YRO-TEL------P_BZN_BZN|10YCS-SERBIATSOV',
                                     'CTY|10YRO-TEL------P!BZN_BZN|10YRO-TEL------P_BZN_BZN|10Y1001C--00003F',
                                     'CTY|10YRO-TEL------P!BZN_BZN|10YRO-TEL------P_BZN_BZN|10YUA-WEPS-----0'],
                              'RS': ['CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YAL-KESH-----5',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YBA-JPCC-----D',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YCA-BULGARIA-R',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YHR-HEP------M',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YCS-CG-TSO---S',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YMK-MEPSO----8',
                                     'CTY|10YCS-SERBIATSOV!BZN_BZN|10YCS-SERBIATSOV_BZN_BZN|10YRO-TEL------P'],
                              'RU': ['CTY|RU!BZN_BZN|10Y1001A1001A49F_BZN_BZN|10Y1001A1001A39I',
                                     'CTY|RU!BZN_BZN|10Y1001A1001A49F_BZN_BZN|10YFI-1--------U',
                                     'CTY|RU!BZN_BZN|10Y1001A1001A49F_BZN_BZN|10YLV-1001A00074',
                                     'CTY|RU!BZN_BZN|10Y1001A1001A49F_BZN_BZN|10Y1001C--00003F',
                                     'CTY|RU!BZN_BZN|10Y1001A1001A50U_BZN_BZN|10YLT-1001A0008Q'],
                              'SE': ['CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A44P_BZN_BZN|10YFI-1--------U',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A44P_BZN_BZN|10YNO-4--------9',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A44P_BZN_BZN|10Y1001A1001A45N',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A45N_BZN_BZN|10YNO-3--------J',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A45N_BZN_BZN|10YNO-4--------9',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A45N_BZN_BZN|10Y1001A1001A46L',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A46L_BZN_BZN|10YDK-1--------W',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A46L_BZN_BZN|10YFI-1--------U',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A46L_BZN_BZN|10YNO-1--------2',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A46L_BZN_BZN|10Y1001A1001A47J',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A47J_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A47J_BZN_BZN|10Y1001A1001A82H',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A47J_BZN_BZN|10YDK-2--------M',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A47J_BZN_BZN|10YLT-1001A0008Q',
                                     'CTY|10YSE-1--------K!BZN_BZN|10Y1001A1001A47J_BZN_BZN|10YPL-AREA-----S'],
                              'SI': ['CTY|10YSI-ELES-----O!BZN_BZN|10YSI-ELES-----O_BZN_BZN|10YAT-APG------L',
                                     'CTY|10YSI-ELES-----O!BZN_BZN|10YSI-ELES-----O_BZN_BZN|10Y1001A1001A63L',
                                     'CTY|10YSI-ELES-----O!BZN_BZN|10YSI-ELES-----O_BZN_BZN|10YHR-HEP------M',
                                     'CTY|10YSI-ELES-----O!BZN_BZN|10YSI-ELES-----O_BZN_BZN|10Y1001A1001A73I',
                                     'CTY|10YSI-ELES-----O!BZN_BZN|10YSI-ELES-----O_BZN_BZN|10Y1001A1001A67D'],
                              'SK': ['CTY|10YSK-SEPS-----K!BZN_BZN|10YDOM-CZ-DE-SKK_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YSK-SEPS-----K!BZN_BZN|10YSK-SEPS-----K_BZN_BZN|10YCZ-CEPS-----N',
                                     'CTY|10YSK-SEPS-----K!BZN_BZN|10YSK-SEPS-----K_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10YSK-SEPS-----K!BZN_BZN|10YSK-SEPS-----K_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10YSK-SEPS-----K!BZN_BZN|10YSK-SEPS-----K_BZN_BZN|10Y1001C--00003F',
                                     'CTY|10YSK-SEPS-----K!BZN_BZN|10YSK-SEPS-----K_BZN_BZN|10YUA-WEPS-----0'],
                              'TR': ['CTY|10YTR-TEIAS----W!BZN_BZN|10YTR-TEIAS----W_BZN_BZN|10YCA-BULGARIA-R',
                                     'CTY|10YTR-TEIAS----W!BZN_BZN|10YTR-TEIAS----W_BZN_BZN|10YGR-HTSO-----Y'],
                              'UA': ['CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10Y1001A1001A51S',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10Y1001A1001A990',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10YPL-AREA-----S',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10YRO-TEL------P',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10Y1001A1001A49F',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001C--00003F_BZN_BZN|10YSK-SEPS-----K',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10YUA-WEPS-----0_BZN_BZN|10YHU-MAVIR----U',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10YUA-WEPS-----0_BZN_BZN|10YRO-TEL------P',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10YUA-WEPS-----0_BZN_BZN|10YSK-SEPS-----K',
                                     'CTY|10Y1001C--00003F!BZN_BZN|10Y1001A1001A869_BZN_BZN|10YPL-AREA-----S'],
                              'UK': ['CTY|GB!BZN_BZN|10YGB----------A_BZN_BZN|10YBE----------2',
                                     'CTY|GB!BZN_BZN|10YGB----------A_BZN_BZN|10YFR-RTE------C',
                                     'CTY|GB!BZN_BZN|10YGB----------A_BZN_BZN|10Y1001A1001A59C',
                                     'CTY|GB!BZN_BZN|10YGB----------A_BZN_BZN|10YNL----------L']}

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
            logging.exception(error, exc_info=True)
            raise error from None
        except requests.HTTPError as error:
            try:
                error_data = json.loads(response.text)
            except ValueError:
                logging.error(error)
                raise error from None
            else:
                if "errors" in error_data:
                    logging.error("post api call bad params "
                                  + error_data["errors"][0]["message"])
                    raise EntsoeApiBadParams(
                        error_data["errors"][0]["message"]) from None
        else:
            return json.loads(response.text)

    @classmethod
    def __get(cls, url, params):
        """
        Low Level API call
        """

        try:
            response = requests.get(url, params=params,
                                    headers=cls.__get_headers)
            response.raise_for_status()
        except (requests.HTTPError, requests.ConnectionError) as error:
            logging.exception(error, exc_info=True)
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

    def transmission_grid_unavailability(self, from_date, to_date, area_type,
                                         country=None,
                                         asset_type=None, outage_type=None,
                                         outage_status=None,
                                         ):
        """
        Implements api method to get unavailability in transmission grid
        """
        if country is None:
            borders = "ALL"
        else:
            if country not in self.countries:
                raise RuntimeError(f"Country code: {country} is invalid")
            if "BORDER_CTA" in area_type:
                borders = self.__cta_borders[country]
            elif "BORDER_BZN" in area_type:
                borders = self.__bzn_borders[country]
        if asset_type is None:
            asset_type = self.asset_type
        if outage_type is None:
            outage_type = self.outage_type
        if outage_status is None:
            outage_status = self.outage_status

        import pprint

        msg = f"session config \n" \
            f"time interval: {from_date} - {to_date}\n" \
            f"asset type   : {pprint.pformat(asset_type, indent=4)}\n" \
            f"outage type  : {pprint.pformat(outage_type, indent=4)}\n" \
            f"outage status: {pprint.pformat(outage_status, indent=4)}\n" \
            f"country: {country} area type: {area_type} \n" \
            f"borders: \n{pprint.pformat(borders, indent=2, )}\n"

        logging.info(msg)
        table_data = []

        params = (
            ('name', ''),
            ('defaultValue', 'false'),
            ('viewType', 'TABLE'),
            ('areaType', area_type),
            ('atch', 'false'),
            ('dateTime.dateTime', f'{from_date} 00:00|UTC|DAY'),
            ('dateTime.endDateTime', f'{to_date} 00:00|UTC|DAY'),
            ('border.values', borders),
            ('assetType.values', [self.asset_type[param] for param in asset_type
                                  if param in self.asset_type]),
            ('outageType.values', [self.outage_type[param] for param in
                                   outage_type if param in self.outage_type]),
            ('outageStatus.values', [self.outage_status[param]
                                     for param in outage_status if param
                                     in self.outage_status]),
        )

        data = {"sEcho": 2,  # what is this ?
                "iColumns": 7,
                "sColumns": "status,nature,unavailabilityInterval,"
                            "inArea,outArea,newNTC,",
                "iDisplayStart": 0,
                "iDisplayLength": self.items_per_page,
                "amDataProp": [0, 1, 2, 3, 4, 5, 6]}

        have = 0  # keep track of  data
        logging.info("start downloading table data\n")
        while True:
            json_data = self.api_call("getDataTableData/", params, data)

            data_frag = self.parse_table_data(json_data)

            have += len(data_frag)
            data.update({'iDisplayStart': have})  # set pagination offset

            # append data
            table_data = table_data + data_frag
            try:
                progress = have / json_data['iTotalRecords']
            except ZeroDivisionError:
                progress = 0

            print(f"[1/3] data progress {round(100 * progress, 2)}%", end="\r")
            logging.info(f"progress [{have} / {json_data['iTotalRecords']}] "
                         f"data")

            if have == json_data['iTotalRecords']:
                print("\n \n")
                logging.info("data  download completed\n\n")
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
    def data_to_df(data):
        raise NotImplementedError("Work in progress")
        """
        Returns a pandas dataframe from  unavailability data in
        transmission grid
        """
        for d in data:
            interval = d['unavailabilityInterval']
            start_date, end_date = API.parse_unavailability_interval(interval)
            d.pop('unavailabilityInterval', None)
            d.update({"dateStart": start_date,
                      "dateEnd": end_date})

        return pd.DataFrame(data)

    @staticmethod
    def parse_unavailability_interval(interval, tz_support=False):
        """
        Parses date interval in data_to_df function
        """
        date_string, tz = tuple(interval.rsplit(" ("))
        tz = tz.replace(")", "").strip()

        start_date, end_date = tuple(date_string.split(" - "))

        start_date = start_date.strip()
        end_date = end_date.strip()

        start_date = datetime.datetime.strptime(start_date, "%d.%m.%Y %H:%M")
        end_date = datetime.datetime.strptime(end_date, "%d.%m.%Y %H:%M")

        if tz_support:
            start_date = start_date.replace(tzinfo=pytz.timezone(tz))
            end_date = end_date.replace(tzinfo=pytz.timezone(tz))

        return [start_date, end_date]

    @staticmethod
    def details_to_df(data):
        """
        Returns a pandas dataframe with details data indexed on detailId
        """
        raise NotImplementedError("Work in progress")

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

        # hack remove duplicate failure status in details
        if details_data.count("Failure") == 2:
            details_data.remove("Failure")

        if len(details_data) != 6:
            # hack fill in missing values in Affected Assets when there are No
            # Affected Assets
            # logging.warning("Row id {} has missing data, fill in "
            #                "missing values".format(detail_id))
            for i in range(6 - len(details_data)):
                details_data.append(details_data[-1])

        # add detailId to each detail table for easy indexing
        details_data.append(detail_id)
        return details_data

    @staticmethod
    def parse_data_details(tables_data):
        """
        Parses data returned from details_grid_unavailability method
        """
        if len(tables_data) != 7:
            raise EntsoeApiExcetpion(f"invalid  size for details : "
                                     f"{tables_data}")
        return {"comments": tables_data[0],
                "reason": tables_data[1],
                "code": tables_data[2],
                "type": tables_data[3],
                "name": tables_data[4],
                "location": tables_data[5],
                "detailId": tables_data[6]
                }

    def curve_grid_unavailability(self, detail_id, offset=0, stop_offset=0,
                                  batch_size=None, batch_progress=None):
        """
        Implements api method getDetailCurve
        """

        timeseries_data = []
        have = offset

        params = (
            ('detailId', detail_id),
        )

        data = {"sEcho": 1,
                "iColumns": 2,
                "sColumns": "mtu,ntc",
                "iDisplayStart": offset,
                "iDisplayLength": self.items_per_page,
                "amDataProp": [0, 1]}

        while True:
            json_curve = self.api_call("getDetailCurve/", params, data)
            curve_frag = json_curve['aaData']
            # pprint.pprint(curve_frag, indent=2)

            have += len(curve_frag)

            timeseries_data = timeseries_data + curve_frag
            data.update({"iDisplayStart": have})

            msg = f"progress [{have} / {json_curve['iTotalRecords']}] " \
                f"{detail_id}"
            msg = f"batch [{batch_progress}/{batch_size}] " \
                  + msg if batch_size else msg
            logging.info(msg)

            if have == json_curve['iTotalRecords']:
                break
            elif have >= stop_offset:
                break

        return timeseries_data

    @staticmethod
    def curve_to_df(data):
        """
        Returns a pandas dataframe from time series data indexed on date
        """

        raise NotImplementedError("Work in progress")

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
        logging.info("start downloading detail data\n")
        total = len(detail_id_list)
        detail_data = []
        for progress, i in enumerate(detail_id_list):
            try:
                detail = api.details_grid_unavailability(i)
                detail = api.parse_data_details(detail)
            except Exception as error:
                logging.error(error)
                raise error from None
            else:
                detail_data.append(detail)
                print(f"[2/3] detail  progress "
                      f"{round(100 * ((progress + 1) / total), 2)}%", end="\r")
                logging.info(f"progress [{progress + 1} / {total}] detail {i}")
                time.sleep(0.5)

        logging.info("detail download completed\n\n")
        print("\n \n")
        return detail_data

    @staticmethod
    def curve_grid_unavailability_batch(api, detail_id_list, days_to_fetch=None,
                                        skip_past_data=False):
        if skip_past_data:
            if len(detail_id_list[0]) is not 3:
                raise RuntimeError("skip_past_data needs time interval")
        total = len(detail_id_list)
        have = 0
        timeseries_data = []

        logging.info("start downloading time series data\n")
        for progress, i in enumerate(detail_id_list):
            print(f"[3/3] time series  progress "
                  f"{round(100 * ((progress + 1) / total), 2)}%", end="\r")
            try:
                if skip_past_data:
                    offset = api.calculate_offset_from_now(i[1], i[2])
                    stop_offset = offset + 60 * days_to_fetch
                    detail = api. \
                        curve_grid_unavailability(i[0], offset, stop_offset,
                                                  batch_progress=progress + 1,
                                                  batch_size=total)
                else:
                    offset = 0
                    stop_offset = 60 * days_to_fetch
                    detail = api. \
                        curve_grid_unavailability(i[0], offset, stop_offset,
                                                  batch_progress=progress + 1,
                                                  batch_size=total)
            except Exception as error:
                logging.error(error)
                raise error from None
            else:

                timeseries_data.append({i[0]: detail})
                have += 1
        print("\n \n")
        logging.info("time series download completed\n\n")
        return timeseries_data

    @staticmethod
    def parse_borders_from_html_code():
        """
        returns  country -  borders dictionary
        """

        borders = {}

        response = requests.get("https://transparency.entsoe.eu/outage-domain/"
                                "r2/unavailabilityInTransmissionGrid/show")

        soup = BeautifulSoup(response.text, 'lxml')
        divs = soup.find_all("div", class_="dv-sub-filter-hierarchic-wrapper")

        for country, div in zip(API.countries, divs):
            inputs = div.find_all("input")
            country_borders = []
            for i in inputs:
                border = i.get("value")
                if 'on' in border:  # ignore this string in borders
                    continue
                country_borders.append(border)
            borders.update({country: country_borders})

        return borders

    @staticmethod
    def calculate_offset_from_now(start_date, end_date):
        if type(start_date) is str and type(end_date) is str:
            start_date = datetime.datetime. \
                strptime(start_date, "%d.%m.%Y %H:%M")
            end_date = datetime.datetime. \
                strptime(end_date, "%d.%m.%Y %H:%M")

        now_date = str(
            datetime.datetime.now()).replace("-", ".").split(" ")[0] + " 00:00"
        now_date = datetime.datetime.strptime(now_date, "%Y.%m.%d %H:%M")

        number_of_datapoints = len(pd.date_range(start_date, end_date,
                                                 freq='H'))
        off_set_time = now_date - start_date
        off_set = len(pd.date_range(start_date, start_date + off_set_time,
                                    freq='H'))
        return off_set
