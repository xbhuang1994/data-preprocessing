

# %%
import os
import time
import pandas as pd
import numpy as np

from utils import time_to_lifetime
class Resampling:
    def __init__(self,file_path,interval):
        self.file_path = file_path
        self.interval = interval
        self.last_time = 0
        self.df_ob = pd.DataFrame(columns=['time','cancel','bs','price','vol','delegate_id'])
        self.df_filled_orders = pd.DataFrame(columns=['price','vol','bs'])
        
    def place_order(self,order):
        snap_time = int(order.lifetime / self.interval) * self.interval
        if order.filled:
            self.filled_order(order)
            pass
        else:
            self.limit_order(order)
        if snap_time != self.last_time:
            self.last_time = snap_time
            self.snap(snap_time,order.time)
            
    def limit_order(self,order):
        
        if order.cancel == 'D':
            try:
                self.df_ob.at[order.delegate_id,'vol'] -= order.vol
            except:
                print('delegate id not found:',order.delegate_id)
            
        else:
            self.df_ob = pd.concat([self.df_ob,pd.DataFrame(order).T])
            self.df_ob.index = self.df_ob['delegate_id']
    
    
    def filled_order(self,order):
        vol = order.vol
        bid_id = order.bid_id
        ask_id = order.ask_id
        try:
            self.df_ob.at[bid_id,'vol'] -= vol
        except:
            print("bid id not found",bid_id)
        try:
            self.df_ob.at[ask_id,'vol'] -= vol
        except:
            print('ask id not found',ask_id)
        self.df_filled_orders = pd.concat([self.df_filled_orders,pd.DataFrame(order).T])
        
    
    def get_ob(self):
        self.df_ob.drop(self.df_ob[self.df_ob.vol == 0].index,inplace=True)
        df_ob = self.df_ob
        df_bids = df_ob.loc[df_ob['bs'] == 'B'][['price','vol']].groupby('price').agg({'vol':'sum',}).reset_index(level=0)
        df_asks = df_ob.loc[df_ob['bs'] == 'S'][['price','vol']].groupby('price').agg({'vol':'sum',}).reset_index(level=0)
        df_bids = df_bids.sort_values(by=['price'],ascending = False)
        df_asks = df_asks.sort_values(by=['price'])
        return df_bids.to_numpy(), df_asks.to_numpy()
    
    def weighted_avg(self,df):
        v = df['price']
        w = df['vol']
        if v.shape[0] > 0:
            s = w.sum()
            w = w.map(lambda x: x / s)
            w = w.to_numpy()
            v = v.to_numpy()
            c = np.average(v,weights=w)
            return int(c),s
        else:
            return 0,0
        
    def snap(self,snap_time,time):
        if time < 93000000 :
            return
        df_buy = self.df_filled_orders.loc[self.df_filled_orders['bs'] == 'B']
        df_sell = self.df_filled_orders.loc[self.df_filled_orders['bs'] == 'S']
        buy_price,buy_vol = self.weighted_avg(df_buy)
        sell_price,sell_vol = self.weighted_avg(df_sell)
        bids,asks = self.get_ob()
        buy_price = buy_price if buy_price > 0 else asks[0][0]
        sell_price = sell_price if sell_price > 0 else bids[0][0]
        # print(time,bids[:5].flatten(),asks[:5].flatten(),buy_price,buy_vol,sell_price,sell_vol)
        self.df_filled_orders.drop(self.df_filled_orders.index, inplace=True)
        
    
    def resampling(self):
        start = time.time()
        path = self.file_path
        orderbook_path = os.path.join(path,"逐笔委托.csv")
        order_path = os.path.join(path,"逐笔成交.csv")
        print("Resampling...",order_path)
        df_order = pd.read_csv(order_path,encoding="gb2312")
        df_orderbook = pd.read_csv(orderbook_path,encoding="gb2312")
        df_order = df_order[['时间','BS标志','成交价格','成交数量','叫卖序号','叫买序号']]
        df_orderbook = df_orderbook[['时间','委托类型','委托代码','委托价格','委托数量','交易所委托号']]
        df_order.columns = ['time','bs','price','vol','bid_id','ask_id']
        df_orderbook.columns = ['time','cancel','bs','price','vol','delegate_id']
        df_order['lifetime'] = df_order['time'].apply(time_to_lifetime)
        df_order['filled'] = 1
        df_order['delegate_id'] = 0
        df_orderbook['lifetime'] = df_orderbook['time'].apply(time_to_lifetime)
        df_orderbook['filled'] = 0
        df_orderbook['bid_id'] = 0
        df_orderbook['ask_id'] = 0
        order_book_all = pd.concat([df_order, df_orderbook],axis=0)
        order_book_all.sort_values(by=['lifetime','filled'], inplace=True)
        order_book_all = order_book_all.reset_index(drop=True)
        
        print("order_book_all shape:",order_book_all.shape)
        order_book_all.iloc[:2000,:].apply(self.place_order,axis=1)
        
        end = time.time()
        print("总共用时{}秒".format((end - start)))
        

def main():
    file_path = r"D:\workspace\stock-strategy\knife\data preprocessing\data\20220701\110038.SZ"
    r = Resampling(file_path,1000)
    r.resampling()
    

if __name__ == '__main__':
    main()

# %%
