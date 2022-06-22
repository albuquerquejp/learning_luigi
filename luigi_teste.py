from pycoingecko import CoinGeckoAPI
import luigi
import pandas as pd


class MyTask1 (luigi.Task):
   
    def output(self):
        return luigi.LocalTarget("bitcoin_data.csv")

    def run(self):
        cg = CoinGeckoAPI()

        bitcoin_data = cg.get_coin_market_chart_by_id(id='bitcoin', vs_currency='usd', days=90)

        df = pd.DataFrame(bitcoin_data)
        
        with self.output().open('w') as csv: 
            csv.write(df.to_csv())
   
if __name__ == '__main__':
    luigi.run()