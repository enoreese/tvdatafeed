import datetime
import asyncio
import enum
import json
import logging
import random
import re
import regex
import string
import pandas as pd
from tqdm import tqdm
from websocket import create_connection
from websocket import WebSocketException, WebSocketConnectionClosedException
from typing import Union, List, Dict, Any
import requests
import json

logger = logging.getLogger(__name__)


class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"


class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
            self,
            username: str = None,
            password: str = None,
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = False

        self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning(
                "you are using nologin method, data you access may be limited"
            )

        self.ws = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):

        if (username is None or password is None):
            token = None

        else:
            data = {"username": username,
                    "password": password,
                    "remember": "on"}
            try:
                response = requests.post(
                    url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                token = response.json()['user']['auth_token']
            except Exception as e:
                logger.error('error while signin')
                token = None

        return token

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    def create_connection(self):
        self.__create_connection()
        return self.ws

    def __create_custom_connection(self, url):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            url, headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            print(m)
        self.ws.send(m)

    @staticmethod
    async def __create_df(raw_data, symbol, add_s_field=False):
        try:
            out = re.search('"s":\[\{(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = re.split("\[|:|,|\]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4])).isoformat()

                row = [ts]

                for i in range(5, 10):

                    # skip converting volume data if does not exists
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))

                    except ValueError:
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            )
            data.insert(0, "symbol", value=symbol)

            if add_s_field:
                s_field = re.findall(r'"\s*(s\d+)"\s*:', raw_data)
                s_field = s_field[0]
                data["s_field"] = s_field

            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")

    def __create_overview_result(self, raw_data, symbol):
        try:
            raw_data = re.sub(r"~m~\d+~m~", ",", raw_data)
            raw_data = f"[{raw_data[1:]}]"
            matches = json.loads(raw_data)
            out = {}
            for group in matches:
                p = group.get("p", [])

                if not p:
                    continue

                if isinstance(p[-1], dict):
                    out.update(p[-1]["v"])

            return out
        except AttributeError as e:
            print(e)
            logger.error("no data, please check the exchange and symbol")

    def __create_overview_result_update(self, raw_data, single_output=True):
        try:
            raw_data = re.findall('"v":\{(.+?)\}~m', raw_data)
            matches = [json.loads("{" + out[:-3] + "}") for out in raw_data]
            if single_output:
                out = [match for match in matches if "business_description" in match][0]
            else:
                out = [match for match in matches if "business_description" in match]

            if not out:
                logger.error("no data, please check the exchange and symbol")
                return None

            return out
        except AttributeError as e:
            print(e)
            logger.error("no data, please check the exchange and symbol")

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
            self,
            symbol: str,
            exchange: str = "NSE",
            interval: Interval = Interval.in_daily,
            n_bars: int = 10,
            fut_contract: int = None,
            extended_session: bool = False,
    ) -> pd.DataFrame:
        """get historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

        self.__send_message(
            "quote_add_symbols", [self.session, symbol,
                                  {"flags": ["force_permission"]}]
        )
        self.__send_message("quote_fast_symbols", [self.session, symbol])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "symbol_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        self.__send_message(
            "create_series",
            [self.chart_session, "s1", "s1", "symbol_1", interval, n_bars],
        )
        self.__send_message("switch_timezone", [
            self.chart_session, "exchange"])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        while True:
            try:
                result = self.ws.recv()
                raw_data = raw_data + result + "\n"
            except Exception as e:
                logger.error(e)
                break

            if "series_completed" in result:
                break

        return self.__create_df(raw_data, symbol)

    async def get_hist_batch(
            self,
            symbols: List[str],
            exchanges: List[str],
            interval: Interval = Interval.in_daily,
            n_bars: int = 10,
            fut_contract: int = None,
            extended_session: bool = False,
            session=None
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """get historical data

        Args:
            symbols (str or list): symbol name or list of symbol names
            exchanges (str or list, optional): exchange or list of exchanges. Defaults to "NSE".
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front,
                                        2 for continuous next contract in front. Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True.
                                            Defaults to False.

        Returns:
            Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
                - Single DataFrame if single symbol is provided
                - Dictionary of {symbol: DataFrame} if multiple symbols are provided
        """
        symbols = [
            self.__format_symbol(
                symbol=symbol, exchange=exchange, contract=fut_contract
            ) for symbol, exchange in zip(symbols, exchanges)
        ]
        symbols_string = ",".join(symbols)

        interval = interval.value

        if not session:
            self.__create_connection()
        else:
            self.ws = session

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

        symbol_dfs = {}
        for i, symbol in enumerate(symbols):
            self.__send_message(
                "resolve_symbol",
                [
                    self.chart_session,
                    f"symbol_{i + 1}",
                    '={"symbol":"'
                    + symbol
                    + '","adjustment":"splits","session":'
                    + ('"regular"' if not extended_session else '"extended"')
                    + "}",
                ],
            )
            self.__send_message(
                "create_series",
                [self.chart_session, f"s{i + 1}", f"s{i + 1}", f"symbol_{i + 1}", interval, n_bars],
            )

            symbol_dfs[symbol] = await self.receive_create_df(symbol)

            self.__send_message(
                "remove_series",
                [self.chart_session, f"s{i + 1}"],
            )

        self.__send_message("switch_timezone", [
            self.chart_session, "exchange"])

        return symbol_dfs

    def is_ws_connected(self):
        try:
            self.ws.recv()  # Attempt to receive data, which updates ws.connected
            return True
        except WebSocketException:
            return False

    async def get_hist_stream(
            self,
            symbols: List[str],
            exchanges: List[str],
            interval: Interval = Interval.in_daily,
            n_bars: int = 10,
            fut_contract: int = None,
            extended_session: bool = False,
            on_message: Any = None,
            params: Dict[str, Any] = None
    ):
        """get historical data

        Args:
            symbols (str or list): symbol name or list of symbol names
            exchanges (str or list, optional): exchange or list of exchanges. Defaults to "NSE".
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front,
                                        2 for continuous next contract in front. Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True.
                                            Defaults to False.

        Returns:
            Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
                - Single DataFrame if single symbol is provided
                - Dictionary of {symbol: DataFrame} if multiple symbols are provided
        """
        symbols = [
            self.__format_symbol(
                symbol=symbol, exchange=exchange, contract=fut_contract
            ) for symbol, exchange in zip(symbols, exchanges)
        ]

        interval = interval.value

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])

        raw_data = ""
        symbol_dfs = {}

        logger.debug(f"getting data for {len(symbols)} symbols...")
        while True:
            # try and check if self.ws is not none and is still open
            if not self.is_ws_connected():
                self.__create_connection()

            for i, symbol in tqdm(enumerate(symbols)):
                chart_session = self.__generate_chart_session()
                session = self.__generate_session()

                self.__send_message("chart_create_session", [chart_session, ""])
                self.__send_message("quote_create_session", [session])
                self.__send_message(
                    "quote_set_fields",
                    [
                        session,
                        "ch",
                        "chp",
                        "current_session",
                        "description",
                        "local_description",
                        "language",
                        "exchange",
                        "fractional",
                        "is_tradable",
                        "lp",
                        "lp_time",
                        "minmov",
                        "minmove2",
                        "original_name",
                        "pricescale",
                        "pro_name",
                        "short_name",
                        "type",
                        "update_mode",
                        "volume",
                        "currency_code",
                        "rchp",
                        "rtc",
                    ],
                )
                self.__send_message(
                    "resolve_symbol",
                    [
                        chart_session,
                        f"symbol_{i + 1}",
                        '={"symbol":"'
                        + symbol
                        + '","adjustment":"splits","session":'
                        + ('"regular"' if not extended_session else '"extended"')
                        + "}",
                    ],
                )
                self.__send_message(
                    "create_series",
                    [chart_session, f"s{i + 1}", f"s{i + 1}", f"symbol_{i + 1}", interval, n_bars],
                )

                try:
                    result = self.ws.recv()
                    await asyncio.sleep(1.5)
                    raw_data = raw_data + result + "\n"
                except Exception as e:
                    logger.error(e)
                    break

                if "series_completed" in result:
                    symbol_df = await self.__create_df(raw_data, symbol, add_s_field=True)
                    symbol_df.reset_index(inplace=True)
                    symbol_dfs[symbol] = symbol_df

                    if on_message:
                        on_message.delay(symbol_df.to_dict('records'), **params)

                    self.__send_message(
                        "remove_series",
                        [chart_session, f"s{i + 1}"],
                    )
                    raw_data = ""

    async def receive_create_df(self, symbol):
        raw_data = ""
        logger.debug(f"getting data for {symbol}...")
        while True:
            try:
                result = self.ws.recv()
                raw_data = raw_data + result + "\n"
            except Exception as e:
                logger.error(e)
                break

            if "series_completed" in result:
                break
        print(raw_data)
        return await self.__create_df(raw_data, symbol)

    def get_overview(
            self,
            symbol: str,
            exchange: str = "NSE",
            fut_contract: int = None,
    ) -> Dict[str, object]:
        """get historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        date_str = datetime.datetime.now().strftime("%Y_%m_%d-%H_%M")
        url = f"wss://data.tradingview.com/socket.io/websocket?from=symbols%2F{symbol.replace(':', '-')}%2Ffinancials-revenue%2F&date={date_str}"
        self.__create_custom_connection(url)

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("quote_create_session", [self.session])

        self.__send_message(
            "quote_add_symbols", [self.session, symbol]
        )
        self.__send_message("quote_fast_symbols", [self.session, symbol])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        while True:
            try:
                result = self.ws.recv()
                raw_data = raw_data + result + "\n"
            except Exception as e:
                logger.error(e)
                break

            if "series_completed" in result:
                break

        # print(f"Raw: {raw_data}")
        return self.__create_overview_result(raw_data, symbol)

    def get_overview_batch(
            self,
            symbols: List[str],
            exchanges: List[str],
            fut_contract: int = None,
    ) -> Dict[str, object]:
        """get historical data

        Args:
            symbols (str): symbol name
            exchanges (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbols = [
            self.__format_symbol(
                symbol=symbol, exchange=exchange, contract=fut_contract
            ) for symbol, exchange in zip(symbols, exchanges)
        ]
        symbols_string = ",".join(symbols)
        symbols_string_encoded = "%2F".join(symbols)

        date_str = datetime.datetime.now().strftime("%Y_%m_%d-%H_%M")
        url = f"wss://data.tradingview.com/socket.io/websocket?from=symbols%2F{symbols_string_encoded.replace(':', '-')}%2Ffinancials-revenue%2F&date={date_str}"
        self.__create_custom_connection(url)

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("quote_create_session", [self.session])

        symbol_dict = {}
        for i, symbol in enumerate(symbols):
            self.__send_message(
                "quote_add_symbols", [self.session, symbol]
            )
            self.__send_message("quote_fast_symbols", [self.session, symbol])

            raw_data = ""

            logger.debug(f"getting data for {symbol}...")
            while True:
                try:
                    result = self.ws.recv()
                    raw_data = raw_data + result + "\n"
                except Exception as e:
                    logger.error(e)
                    break

                if "series_completed" in result:
                    break

            # print(f"Raw-{symbol}: {raw_data}")

            symbol_dict[symbols[i]] = self.__create_overview_result_update(raw_data, symbol)

        return symbol_dict

    async def get_overview_stream(
            self,
            symbols: List[str],
            exchanges: List[str],
            fut_contract: int = None,
            on_message: Any = None,
            params: Dict[str, Any] = None
    ):
        """get historical data

        Args:
            symbols (str): symbol name
            exchanges (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbols = [
            self.__format_symbol(
                symbol=symbol, exchange=exchange, contract=fut_contract
            ) for symbol, exchange in zip(symbols, exchanges)
        ]
        symbols_string_encoded = "%2F".join(symbols)

        date_str = datetime.datetime.now().strftime("%Y_%m_%d-%H_%M")
        url = f"wss://data.tradingview.com/socket.io/websocket?from=symbols%2F{symbols_string_encoded.replace(':', '-')}%2Ffinancials-revenue%2F&date={date_str}"
        self.__create_custom_connection(url)

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("quote_create_session", [self.session])

        raw_data = ""
        quoted, end_session = False, False
        quoted_tickers = list(set(symbols))
        while True:
            if (len(quoted_tickers) == 0) or end_session:
                break

            for i, symbol in enumerate(list(quoted_tickers)):
                if quoted:
                    continue
                self.__send_message(
                    "quote_add_symbols", [self.session, symbol]
                )
                self.__send_message("quote_fast_symbols", [self.session, symbol])

                logger.debug(f"getting data for {symbol}...")
                await asyncio.sleep(1.5)
            else:
                quoted = True
                try:
                    result = self.ws.recv()
                    await asyncio.sleep(1.5)
                    raw_data = raw_data + result + "\n"
                except WebSocketException as e:
                    logger.error(e)
                    self.__create_custom_connection(url)
                    self.__send_message("set_auth_token", [self.token])
                    self.__send_message("quote_create_session", [self.session])

                except Exception as e:
                    logger.error(e)
                    break

                sym_list = self.__create_overview_result_update(raw_data, single_output=False)
                if sym_list is not None:
                    tickers = [tick["symbol-proname"] for tick in sym_list]

                    quoted_tickers = [tick for tick in quoted_tickers if tick not in tickers]

                    if len(quoted_tickers) == 0:
                        end_session = True

                    if on_message:
                        on_message.delay(sym_list, **params)

                    raw_data = ""

    def search_symbol(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)

        symbols_list = []
        try:
            resp = requests.get(url)

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    tv = TvDatafeed()
    # print(tv.get_hist("KCB", "NSEKE"))
    # print(tv.get_overview(
    #     "KCB",
    #     "NSEKE",
    # ))
    # print(asyncio.run(tv.get_hist_stream(
    #     symbols=["MTNN", "GTCO"],
    #     exchanges=["NSENG", "NSENG"]
    # )))

    print(asyncio.run(tv.get_overview_stream(
        symbols=["MTNN", "GTCO"],
        exchanges=["NSENG", "NSENG"]
    )))

    # print(tv.get_overview_batch(
    #     symbols=["MTNN", "GTCO"],
    #     exchanges=["NSENG", "NSENG"]
    # ))
    # print(tv.get_hist("NIFTY", "NSE", fut_contract=1))
    # print(
    #     tv.get_hist(
    #         "EICHERMOT",
    #         "NSE",
    #         interval=Interval.in_1_hour,
    #         n_bars=500,
    #         extended_session=False,
    #     )
    # )
