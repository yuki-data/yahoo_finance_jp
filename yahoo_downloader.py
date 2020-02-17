"""
yahooファイナンス日本版から株価データを取得するライブラリ。

create: 170422
update: 170422

yahoo_multi_download()がメインの機能。
これで株価データをダウンロードしてcsv保存できる。
複数銘柄の連続ダウンロードができる。

YahooJpStockHistoricalは単一銘柄のダウンロード。
csv保存せず、dataframeにとどめたいときに便利。
adj closeの補正前データが必要な場合にも使える。

adjust_yahoo_ohlcは、yahoo finance米国版のデータでも同様に使用できる補正用関数。

YahooLocalPathは、google_fincaneでダウンロードしたときの命名規則に一致させるための
ファイル名生成ツール。

"""

import datetime
import time
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from .google_finance import LocalDataPath


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


def yahoo_multi_download(symbol_list, start=None, end=None, period_days=10, folder_path=None, pause=0.01):
    """複数銘柄の株価データをダウンロード

    複数銘柄をダウンロードし、csvで保存する。
    個々の銘柄のダウンロードにYahooJpStockHistorical.write_csv()を使用。

    保存ファイル名: google_finance.LocalDataPathの命名規則に従う。
        例えば、symbol_code=9022, interval=300, period="3d"で2017-03-01に作成したファイルパスは、
        code_9022_i_300_p_3d_2017-03-01_16.csvのようになる。

    Parameters:
    - symbol_list: list of int, 銘柄コード(整数)をリストにしたもの
    - folder_path: ダウンロードしたデータをcsvで保存するフォルダパス。Noneならintradayフォルダに保存。
    - end: datetime.date or str,
        データ取得最終日。Noneにすると本日にセット。
        '2014-12-01'のような文字列形式にしてもいいし、datetime.dateにしてもいい。
    - start: datetime.date or str,
        データ取得開始日。 Noneにすると、endからperiod_days遡った日
    - period_days: int, 取得期間日数。startがNoneのときのみ有効。 startにendからperiod_days遡った日をセット
    - pause: float or int, 各銘柄のリクエストごとの待ち時間(単位は秒)。

    Returns:
    - request_result: dict of lists,
        リクエスト成功と失敗の銘柄コードのリストを、辞書型でまとめて返す。
        keys {"request": 全銘柄, "passed": 成功したリスト, "failed": 失敗したリスト},

    Usage:
        # 銘柄コード[9020, 7203, 9022]についてダウンロードしcsvで保存する場合
        # 保存先のフォルダを指定
        folder_yahoo = "path_to_folder"
        result = yahoo_multi_download(symbol_list=[9020, 7203, 9022], folder_path=folder_yahoo, period_days=250)
    """
    if not isinstance(symbol_list, list):
        raise ValueError("The argument has to be list-type.")

    list_failed = []
    list_passed = []

    with requests.Session() as s:
        for i in symbol_list:
            with YahooJpStockHistorical(symbol_code=i, start=start, end=end,
                                        period_days=period_days, request_session=s) as ystock:
                filename = ystock.autogenerate_filename(data_folder_path=folder_path)
                try:
                    ystock.write_csv(filename=filename)
                    list_passed.append(ystock._symbol_code)
                except requests.exceptions.Timeout:
                    msg = "Timeout error: faild to read symbol {0}".format(ystock._symbol_code)
                    warnings.warn(msg)
                    list_failed.append(ystock._symbol_code)
                except IOError:
                    msg = "IOError: Faild to read symbol {0}".format(ystock._symbol_code)
                    warnings.warn(msg)
                    list_failed.append(ystock._symbol_code)
            time.sleep(pause)

    request_result = {"request": symbol_list, "passed": list_passed, "failed": list_failed}

    return request_result


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
                 request_session=None):
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

        self._pause_single_table = 0.01  # page_numを1つ増加するときの待ち時間(秒)
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

        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
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

    def autogenerate_filename(self, data_folder_path=None, save_to_cwd=False):
        """データをcsv保存するときのファイルパスを機械的に生成。
        例えば、code_7203_i_86400_p_30d_2017-04-23_2.csv
        このメソッドは。パスを生成するだけ。

        - data_folder_path: str,
            ファイルを保存するフォルダのパス。
            Noneにすると、google_finance.LocalDataPathのデフォルトパス_data_folderになる。
        - save_to_cwd: bool, Trueにするとカレントディレクトリに保存。

        Returns: str, ファイルパス
        """
        ypath = YahooLocalPath(symbol_code=self._symbol_code,
                               date_end=self._date_end,
                               date_start=self._date_start,
                               data_folder_path=data_folder_path)
        filepath = ypath.create_filepath(save_to_data_folder=(not save_to_cwd))
        return filepath

    def close(self):
        self._request_session = None
        self._df_cached = None


class YahooLocalPath(LocalDataPath):
    """YahooJpStockHistoricalインスタンスから保存先のファイルパスを自動生成する。
    google_finance.LocalDataPathと同様の命名規則になるように、差異のある部分を調整している。"""
    # yahooファイナンスでは日足のみなのでintervalは固定
    _interval = 60 * 60 * 24

    def __init__(self, symbol_code, date_end, date_start, data_folder_path=None):
        """data_folder_pathを指定すると、google_finance.LocalDataPathとは異なるフォルダに保存できる"""
        self._data_folder = data_folder_path or self._data_folder
        period = str((date_end - date_start).days) + "d"
        super().__init__(symbol_code=symbol_code, interval=self._interval, period=period)
