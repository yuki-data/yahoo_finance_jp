"""
yahooファイナンス日本版から株価データを取得するライブラリ。
"""

import datetime
import time
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup


def adjust_yahoo_ohlc(df, inplace=False):
    """yahooファイナンスの株価データdataframeの補正を全てのohlcデータに反映する。

    - df: pandas dataframe, ohlc株価データ。yahooファイナンスから取得し、Adj Closeのカラムが存在する「未補正」のデータ。
    - drop_adj_close: bool, 戻り値のdfから、Adj Closeを除外するか
    - inplace: bool, 入力dfそのものを書き換えるか(True)、新規dfを返すか(False)

    Returns: dataframe, Adj Closeの補正を済ませたデータ
    """
    if inplace is False:
        df = df.copy()
    df["Open"] = df["Open"] * df["Adj Close"] / df["Close"]
    df["High"] = df["High"] * df["Adj Close"] / df["Close"]
    df["Low"] = df["Low"] * df["Adj Close"] / df["Close"]
    df["Close"] = df["Adj Close"]
    df.drop("Adj Close", axis=1, inplace=True)
    return df


class YahooJpStockHistorical():
    """
    Yahooファイナンス日本語版から株価データをダウンロードする。
    3年分のデータのダウンロードに約8秒かかる。

    Usage:
    # 単一銘柄のデータをダウンロードする場合
    ystock = YahooJpStockHistorical(7203, start="2014-01-01", end="2017-04-01")
    df = ystock.get_stockdata()
    # adj closeの補正を適用する
    df = adjust_yahoo_ohlc(df)

    # 本日を終点として、20日前からのデータを取得する
    ystock = YahooJpStockHistorical(7203, period_days=20)

    # sessionを転用する場合
    ystock = YahooJpStockHistorical(7203, request_session=requests.Session())
    """

    def __init__(self, symbol_code=7203, start=None, end=None, period_days=10,
                 request_session=None, pause_single_table=2):
        """
        - symbol_code: int, 銘柄コード
        - end: datetime.date or str,
            データ取得最終日。Noneにすると本日にセット。
            '2014-12-01'のような文字列形式にしてもいいし、datetime.dateにしてもいい。
        - start: datetime.date or str,
            データ取得開始日。 Noneにすると、endからperiod_days遡った日
        - period_days: int, 取得期間日数。startがNoneのときのみ有効。 startにendからperiod_days遡った日をセット
        - request_session: requests.session,
            外部からrequests.Session()を渡したら、それを使う。Noneにすると、内部で作成する。
        """

        self._symbol_code = symbol_code
        end = end or datetime.date.today()
        self._date_end = pd.Timestamp(end)
        start = start or (self._date_end - datetime.timedelta(days=period_days))
        self._date_start = pd.Timestamp(start)
        # intervalは、日足のみとする。今後、週足に対応するかも。
        self._interval = "d"

        if self._date_end <= self._date_start:
            raise ValueError("end date should be later than start date.")

        self._pause_single_table = pause_single_table  # page_numを1つ増加するときの待ち時間(秒)
        self._df_cached = None  # 一時的にdfをキャッシュする
        self._request_session = request_session
        # request sessionからrequestする場合のrequest条件
        # 接続失敗から再リクエストまでの待ち時間(秒)
        self.pause_for_retry = 0.001
        # 再リクエスト回数
        self.retry_count = 2
        # request timeout(秒)
        self.request_timeout = 5

    def __enter__(self):
        """with文のasで受け取るオブジェクトを返す"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """with文から抜けるときに実行する処理"""
        self.close()

        # エラーのあるときのみ実行する処理
        if exc_type is not None:
            print("エラー内容:", exc_value)

    def base_url(self):
        return ('http://info.finance.yahoo.co.jp/history'
                '?code={symbol_code}.T&{start_str}&{end_str}&tm={interval}&p={page_num}')

    def query_param(self):
        start, end = self._date_start, self._date_end
        d = {
            "symbol_code": self._symbol_code,
            "interval": self._interval,
            "start_str": 'sy={0}&sm={1}&sd={2}'.format(start.year, start.month, start.day),
            "end_str": 'ey={0}&em={1}&ed={2}'.format(end.year, end.month, end.day),
            "interval": self._interval
        }
        return d

    def create_url_basepage(self):
        """base_urlにpage_numに以外のパラメータをセットしてurlを返す"""
        baseurl_str = self.base_url()
        param = self.query_param()
        return baseurl_str.format(**param, page_num="{page_num}")

    def get_stockdata(self, force_request=False):
        """株価データをdataframeで返す

        株価データをダウンロードして、dataframe形式に変換する。
        yahooの複数ページにデータがわたっているときには、全ページを掃引して全データを結合してから返す。
        一度ダウンロードしていたら、キャッシュしたdataframeを返す。

        - force_request: bool, データをキャッシュ済みでも再リクエストするか

        Returns: pandas dataframe, yahooファイナンスからダウンロードした株価データのdataframe
        """
        if (self._df_cached is None) or (force_request is True):
            baseurl_str = self.create_url_basepage()
            df = self._get_stock_all_tables(baseurl_str, pause_single_table=self._pause_single_table)
            self._df_cached = self.organize_df(df)
        return self._df_cached

    def organize_df(self, df):
        """dataframeのカラム名と年月日を日本語から米国版表記に変更して返す。
        yahooからダウンロードした直後の株価データはカラム名と年月日が日本語。"""
        if not all(df.columns == ['日付', '始値', '高値', '安値', '終値', '出来高', '調整後終値*']):
            warnings.warn("カラム名が標準形式と異なる")
        if not df.notnull().all().all():
            warnings.warn("nullデータが含まれる")

        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj_Close']
        df.Date = pd.to_datetime(df.Date, format='%Y年%m月%d日')
        return df.sort_values("Date")

    def _get_stock_single_table(self, url):
        """個々のダウンロードページからテーブルデータをダウンロードしてdataframeにて返す

        - url: str, ダウンロードページの完全なurl

        Returns: pandas dataframe, 1つの銘柄のデータ。単一ページのデータのみ。
        """
        txt = self._request_stock_data(url)
        soup = BeautifulSoup(txt, "lxml")
        # 株価データは、該当urlのhtmlのtable.boardFin要素にあるはず。この要素は1つしかないはず。
        tables = soup.select("table.boardFin")
        # もし、table.boardFin要素は1つという想定に反したらエラーを出す
        if not len(tables) == 1:
            raise IOError("data style does not match standard format")
        table = tables[0]
        # table要素内部の構文解析は、pd.read_htmlを使う
        df = pd.read_html(str(table), header=0)[0]
        return df

    def _get_stock_all_tables(self, baseurl_str, pause_single_table=0.01):
        """複数のダウンロードページから順次テーブルデータをダウンロードしてdfにし、結合してから返す。

        - baseurl_str: str, yahooファイナンス株価データページのurl(pege_numのみフォーマットされていない状態)
        - pause_single_table: int, page_numを1足すときの待ち時間

        Returns: pandas dataframe, 1つの銘柄のデータ。複数ページのデータを全て結合済み。
        """
        p = 1
        table_list = []

        # ７年分のデータを取得してもページは100に達しないので、200未満にしても充分
        while p < 200:
            # 株価データを取得できる完全なurl
            url = baseurl_str.format(page_num=p)
            # 個々のページの株価データのdf
            df_table = self._get_stock_single_table(url)
            # 株価データが存在しないページに到達したらループから出る
            if len(df_table) == 0:
                break
            table_list.append(df_table)
            p += 1  # 次のページにすすむ
            time.sleep(pause_single_table)
        return pd.concat(table_list, ignore_index=True)

    def _request_stock_data(self, url):
        """requestsでデータをダウンロードし、html形式のテキストデータを返す"""

        # インスタンス作成時点で外部からrequsets.sessionを受け取っていないならここで生成する
        # 既にsessionをキャッシュしていれば、それを使う
        if self._request_session is None:
            self._request_session = requests.Session()

        s = self._request_session

        for i in range(self.retry_count + 1):
            # timeout秒以内にsever応用がなければエラー
            response = s.get(url, timeout=self.request_timeout)
            if response.status_code == requests.codes.ok:
                return response.text
            time.sleep(self.pause_for_retry)

        raise IOError("Requests Error: http bad status")

    def write_csv(self, filename="temp_stockdata_file.csv", encoding="utf-8", index=False, adjust_ohlc=True):
        """Export csv file.

        pandas dataframeに変換済みの株データをcsvで書き出す。
        dataframe未作成ならここで作成する。

        - filename: str, 保存ファイルパス
        - encoding: 書き出すcsvのエンコード
        - index: csvにindexも含めるか
        - adjust_ohlc: yahooファイナンスのAdj Closeの補正を全データに適用するか
        """

        df = self.get_stockdata()
        if adjust_ohlc is True:
            df = adjust_yahoo_ohlc(df)
        df.to_csv(path_or_buf=filename, encoding=encoding, index=index)
