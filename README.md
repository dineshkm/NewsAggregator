# NewsAggregator
It will aggregate the news from different websites and shows in one place

Softwares used:
1. Python 2
2. Apache Cassandra 2.0.3


Libraries used:
1. BeautifulSoup, Pycassa for Python

Steps to Run:
1. Download and run apache cassandra 2.0.3
2. Start NewsAggregator.py, it is a HTTP Server which runs on port 8800 (by default).
3. While running it will take news from (TechCrunch.com, TechGig.com as of now) every 10 mins.
4. If you send http request "http://host:8800/techcrunch", you will get latest news from Techcrunch.com
