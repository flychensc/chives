@cd "D:\Users\flych\Source\chives\rqalpha"

:: -18.2%
:: -18.1%
:: 1.4%
:: 6.2%
:: 16.1%
::rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-3-10 --benchmark 600048.XSHG -p -l verbose

:: 35.3%
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 600048.XSHG -p -l verbose
:: bad (-22.1%)
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 600694.XSHG -p -l verbose
:: 28.4%
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 601398.XSHG -p -l verbose
:: bad (-1%)
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 601668.XSHG -p -l verbose
:: bad (-4.3%)
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 601390.XSHG -p -l verbose
:: bad (-11.4%)
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 000887.XSHE -p -l verbose
:: 24.8%
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 002627.XSHE -p -l verbose
:: 73.5%
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 002271.XSHE -p -l verbose
:: 0.5%
rqalpha run -f money_back.py --account stock 80000 -cm 20 -s 2015-06-01 -e 2017-12-20 --benchmark 000876.XSHE -p -l verbose

@pause
