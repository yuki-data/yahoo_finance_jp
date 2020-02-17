# yahoo finance日本版 株価ダウンローダー
[Yahoo!ファイナンス](https://finance.yahoo.co.jp/)から株価をダウンロードするツール。
コードはPythonで書いてあります。

## 使い方
例えば、トヨタ自動車(銘柄コード:7203)の株価を、直近の１０日分取得したい場合は以下のようにします。

    # 本日を終点として、20日前からのデータを取得する
    ystock = YahooJpStockHistorical(7203, period_days=10)
    df = ystock.get_stockdata()

これで、PandasのDataFrameとして株価データを取得できます。

![yahooファイナンスからのデータのダウンロード - Jupyter Notebook - Gyazo](https://gyazo.com/572b09dcd6af892a601b95ee627aa6d7.png)

