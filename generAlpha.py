import pandas as pd
from pandasgui import show
import os
import yfinance as yf
from datetime import datetime, timedelta

amele = sorted(os.listdir('/home/gun/Documents/Amele/RaporTarihleri/'))

#Gets 1 row from the data frame or if index is out of bounds returns a pd.series with 0 so program keeps running
def get_row_or_zero(df, row_index):
    if row_index < df.shape[0]:
        return df.iloc[row_index]
    else:
        return pd.Series([0] * df.shape[1], index=df.columns)

#Lognest_row is used to get each row till the file with the oldest date.
num_rows = []
for i in amele:
    file = pd.read_excel(f'/home/gun/Documents/Amele/RaporTarihleri/{i}').T
    file = file[1:]
    num_rows.append(len(file))
longest_row = max(num_rows)

for row in range(longest_row -4): #Row -4 because son marjlarin 4 ceyrek oncesi yok
    pairs = {}
    portfollio = pd.DataFrame()

    for i in amele: #Sort files according to their  dates so when using yfinance they are in order
        stockName = os.path.splitext(i)[0]
        file = pd.read_excel(f'/home/gun/Documents/Amele/RaporTarihleri/{i}').T
        file = file[1:]
        pairs[stockName] = file.iloc[0]

    date_obj = {key: pd.to_datetime(value, errors='coerce') for key, value in pairs.items()}        
    sorted_pairs = dict(sorted(date_obj.items(), key=lambda item: item[1].iloc[0]))

    stocks = list(sorted_pairs.keys())
    for file_name, date in sorted_pairs.items():
        data_frame = pd.read_excel(f'/home/gun/Documents/CalculatedRatios/{file_name}.xlsx')
        data_frame = data_frame.rename(columns={'Unnamed: 0': 'Tarih'})
        copy_df = data_frame.shift(-4) #copy df created in order to calculate marj ratios
        copy_df = copy_df.fillna(0)

        #Calculating Ratios
        data_frame['F/K'] = data_frame['F/K'].apply(lambda x: 0 if x >= 100 or x <= 0 else x)        
        min_fk = 0
        max_fk = 99.9999
        normalize_fk = (data_frame['F/K'] - min_fk) / (max_fk - min_fk)
        fk_puan = 1 - normalize_fk
        fk_puan = fk_puan.apply(lambda x: 0 if x >= 1 or x <= 0 else x)
        data_frame['F/K Puan'] = fk_puan

        data_frame['PD/DD'] = data_frame['PD/DD'].apply(lambda x: 0 if x >= 100 or x <= 0 else x)
        min_pd_dd = 0
        max_pd_dd = 99.9999
        normalize_pd_dd = (data_frame['PD/DD'] - min_pd_dd) / (max_pd_dd - min_pd_dd)
        pd_dd_puan = 1 - normalize_pd_dd
        pd_dd_puan = pd_dd_puan.apply(lambda x: 0 if x >= 1 or x <= 0 else x)
        data_frame['PD/DD Puan'] = pd_dd_puan

        data_frame['Cari Oran'] = data_frame['Cari Oran'].apply(lambda x: 200 if x >= 200 else (0 if x <= 0 else x))
        min_cari_oran = 0
        max_cari_oran = 200.01
        normalize_cari_oran = (data_frame['Cari Oran'] - min_cari_oran) / (max_cari_oran - min_cari_oran)
        data_frame['Cari Oran Puan'] = normalize_cari_oran

        kaldirac_orani_puan = 1 - data_frame['Kaldıraç Oranı']
        kaldirac_orani_puan = kaldirac_orani_puan.apply(lambda x: 0 if x <= 0 else (1 if x >= 1 else x))
        data_frame['Kaldıraç Oranı Puan'] = kaldirac_orani_puan

        brut_kar_ceyrek_percentage_change = (data_frame['Brüt Kar Marjı Çeyreklik'] - copy_df['Brüt Kar Marjı Çeyreklik']) /  data_frame['Brüt Kar Marjı Çeyreklik']
        data_frame['Brüt Kar Marjı Çeyreklik Puan'] = brut_kar_ceyrek_percentage_change.apply(lambda x: 0 if x <= 0 else (1 if x >= 1 else x))

        brut_kar_yil_percentage_change = (data_frame['Brüt Kar Marjı Yıllık'] - copy_df['Brüt Kar Marjı Yıllık']) /  data_frame['Brüt Kar Marjı Yıllık']
        data_frame['Brüt Kar Marjı Yıllık Puan'] = brut_kar_yil_percentage_change.apply(lambda x: 0 if x <= 0 else (1 if x >= 1 else x))

        net_kar_ceyrek_percentage_change = (data_frame['Net Kar Marjı Çeyreklik'] - copy_df['Net Kar Marjı Çeyreklik']) /  data_frame['Net Kar Marjı Çeyreklik']
        data_frame['Net Kar Marjı Çeyreklik Puan'] = net_kar_ceyrek_percentage_change.apply(lambda x: 0 if x <= 0 else (1 if x >= 1 else x))

        net_kar_yil_percentage_change = (data_frame['Net Kar Marjı Yıllık'] - copy_df['Net Kar Marjı Yıllık']) /  data_frame['Net Kar Marjı Yıllık']
        data_frame['Net Kar Marjı Yıllık Puan'] = net_kar_yil_percentage_change.apply(lambda x: 0 if x <= 0 else (1 if x >= 1 else x))

        ozkaynak_percentage_change = (data_frame['Özkaynak Karlılığı'] - copy_df['Özkaynak Karlılığı']) /  data_frame['Özkaynak Karlılığı']
        data_frame['Özkaynak Karlılığı Puan'] = ozkaynak_percentage_change.apply(lambda x: 0 if x <= 0 else (1 if x >= 1 else x))

        data_frame = data_frame[:-4]

        in_query = get_row_or_zero(data_frame, row)
        in_query = in_query.to_frame().T
        in_query.set_index(pd.Index([file_name]), inplace=True)

        last_trading_day = '2024/02/15'
        #Checking Criterias
        if ((in_query['F/K Puan'] > 0.98) & (in_query['PD/DD Puan'] > 0.98)).any():
            yfstock = file_name + '.IS'
            file_buy_date = pd.read_excel('/home/gun/Documents/Amele/RaporTarihleri/{}.xlsx'.format(file_name)).T
            file_buy_date = file_buy_date[0].iloc[1:]
            file_sell_date = pd.read_excel('/home/gun/Documents/Amele/RaporTarihleri/{}.xlsx'.format(file_name)).T

            if row in file_buy_date.index: #Checking If Buy Date Exists In File
                raw_date = file_buy_date[row]
                raw_date = datetime.strptime(raw_date, '%Y/%m/%d').strftime('%Y-%m-%d')
                date = datetime.strptime(raw_date, '%Y-%m-%d')
                buy_date = date + timedelta(days=1)
                
                file_sell_date[0].iloc[0] = last_trading_day
                raw_sell_date = file_sell_date[0].iloc[row]
                raw_sell_date = datetime.strptime(raw_sell_date, '%Y/%m/%d').strftime('%Y-%m-%d')
                sell_date = datetime.strptime(raw_sell_date, '%Y-%m-%d')
                sell_date = sell_date + timedelta(days=1)

                #Checking if the buy date after the report release is weekend or not and adjust buy date
                while buy_date.weekday() >= 5:
                    days_until_monday = (7 - buy_date.weekday()) % 7
                    buy_date += timedelta(days=days_until_monday)
                buy_date_str = buy_date.strftime('%Y-%m-%d')

                #Checking if the sell date after the report release is weekend or not and adjust sell date
                while sell_date.weekday() >= 5:
                    days_until_monday2 = (7 - sell_date.weekday()) % 7
                    sell_date += timedelta(days=days_until_monday2)

                data = yf.download(yfstock, start=buy_date_str)['Close']

                #Adjusting Buy Date According To YF_Dates If Buy Date Is Abnormal Holiday
                buy_day_difference = (data.index[0] - buy_date).days

                if buy_date in data.index:
                    buy_date = buy_date.strftime('%Y-%m-%d')
                    buy_price = round(data.iloc[0], 2)

                #Get The Closest Trading Date in YF, limited to 10 days to prevent loop for the earliest date in the file
                elif 0 < buy_day_difference <= 10:
                    buy_date = data.index[0]
                    buy_date = buy_date.strftime('%Y-%m-%d')
                    buy_price = round(data.iloc[0], 2)

                else:
                    buy_price = 0
                
                #Adjusting Sell Date According To YF_Dates If Sell Date Is Abnormal Holiday
                closest_index = min(filter(lambda date: date > sell_date, data.index))
                sell_day_difference = (closest_index - sell_date).days
                closest_index = datetime.strftime(closest_index, '%Y-%m-%d')

                if sell_date in data.index: #Checking If YF_Data Has Older Date
                    sell_price = round(data[sell_date], 2)
                    sell_date = sell_date.strftime('%Y-%m-%d')

                #Get The Closest Trading Date in YF, limited to 10 days to prevent loop for the earliest date in the file
                elif 0 < sell_day_difference <= 10:
                    sell_date = closest_index
                    sell_price = round(data[closest_index], 2)

                else:
                    sell_price = 0

                # print(yfstock)
                # print(buy_date)
                # print(buy_price)
                # print(sell_date)
                # print(sell_price)

                position_pnl = ((sell_price - buy_price) / buy_price) * 100

                in_query['Alış Tarihi'] = buy_date
                in_query['Alış Fiyatı'] = buy_price
                in_query['Satış Tarihi'] = sell_date
                in_query['Satış Fiyatı'] = sell_price
                in_query['Kar/Zarar'] = position_pnl

            else:
                position_pnl = 0
            
            portfollio = portfollio._append(in_query)

        else:
            continue

        portfollio = portfollio.sort_values(by='Alış Tarihi', ascending=True)
    show(portfollio)
    # break
