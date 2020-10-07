# golang-scraping

(Items to be acquired)

Code for https://info.finance.yahoo.co.jp/ranking/?kd=4&mk=1&tm=d&vl=a (currently 3875 entries)

Companies with the above code.
https://tyn-imarket.com/stocks/6096
(四半期)
決算期、四半期、売上高、営業利益、経常利益、純利益、EPS、売上高前年比、営業利益前年比、経常利益前年比.
And the date at the top right of the pdf link. The link itself is not necessary.
(累計 / 通期) is not required.

「終値」 of the most recent date prior to the date in the top right corner of the pdf link.
https://stocks.finance.yahoo.co.jp/stocks/history/?code=4565.T.

This is one record.
Number of companies this record is needed. (Now that's 3875 records).

I need the code to get the data and the code to INSERT it.
When I restart the program I want to be able to start from where scraping is not finished.
I would like to be able to select only null columns and start scraping.
