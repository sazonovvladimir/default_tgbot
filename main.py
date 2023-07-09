from telebot import TeleBot
import requests
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import time
import datetime as dt
import numpy as np
from background import keep_alive
import logging
from lxml import html

logging.basicConfig(level=logging.INFO)
TOKEN = '5966527940:AAF1n1_Yw1p3bLjAvEFvNVdPquTWaZnZ8WA'
bot = TeleBot(TOKEN)


def load_df_to_gsheets(data_frame, doc, title, force=False, formats=[], index=False):
    data = {
        "type": "service_account",
        "project_id": "gifted-decker-309314",
        "private_key_id": "b37757f54ad8d9ccaa7a5b838d7076550ae561c9",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCnq/imHWvqi2hw\nqRiskpX/gPvE8aglytajcV+mBmL2TZqfyUzahXRimQoMBmvT3bmEYiqmEP8cyP9e\n35ckMSITH9wEScGQWK79SmPZ5g4XDT8ig/prHDU004OA3RayKSKFylmzyBL4vdP9\n7zEURNkTRR81bDSgRo0YlthPtaI8eFa7iJ7ymoEbh0lww8xH830Hku1OA0SzyAS5\nur2ZXqxO113PoBQKWiwHuBG3PbYyOHdqRKZV5nJ2drUhDU0tHF5CnjspFWDU0uPg\nlwNrGIB2Og5ffbrZgevqiDhWHaduBTRkmdbxNhqhsAvTfECWura2SlqWh79iZZoR\nwkv0RR8dAgMBAAECggEABl5zt6oFPMe5p0Xh23RGx4mYhqoAD03XV1mc636LApYY\nc7QlLv73JWOj/3VWLs1zGMJt1X8ILomIAb+QF/gLaYFbs+7LsA/Et3eJUXAtET0X\nxj52V0LScCsOPLZCzXB4kKJxQ9eR7B1Lalx/ZCi1VWENYKC3Ywf30uF8xMWLRkaw\nEWO/F7OmDiz+BuaTVP/T2GjqW48Shno+dnarhUYRCq3iWzqCbgKfCab47lgK9b4x\nTgjY1MxoH8IUFQ8qZMWVEOT3kvm0KvbBTx1KWJiRLd+5RTIcJgelpIzu2IFLW4v8\n2LMxt8QPd9k82HPmzNE6Ln12iFYcX1zM18FMWc3YoQKBgQDqCzed99VkpuJcZEXH\nsQ87sSV77Ub0cE4oAnPq8+CvMmaZlmYxCA07JZ7KicwM1R+bj1Ca//uJask/NC/p\nOfzds8LCuCkbpx3wpLdIyAoTt2sELViSiDlaAAjIFKakybQw1cOlNHFSf/zaIams\n7UpVC7ShFB7670RAWVgisyFwOQKBgQC3ZsRBBVj5I6VLwE+isS5AXfrN+/6NRWRV\nnpUVKqVCnP4+oMExQUBj2KhIla5oCy7MkjEE7z4Cgu0M39dBhtGRtATS9poMYrIl\nPwVR1pDIQ+N4AzE3MZxFYrx2N19fRbaDhKGU4Br5tTz8FlHCS8svkrjprYjCPdKo\n6KFaRrFeBQKBgCYWJzncv+w+QC3632QsyybSoB/3sAlNUVqvc3+zqke4cvvhfsXR\n4p1SdPHO1Nbtw9QD0YE30Q4+w3s2melhV2YYv8QCRiZK3tNvaqg8bW9h9NdcLcLC\nylp8EPHGcov0iw87ajgzPZHIZDR3L+6FOwh1/DIOXTBGyZuTLoWPEwz5AoGBAJKI\n5qd46VTuWSTEPBymamj9beXkwMcJOZh7Q2yNDUvC+hT6BkIfe54LPrH3/kidHsBO\n0iG6MZZ6G4Lc2jU9zfYXmn8gj4bz2JiP2OVBhZ6tN3LtQgZyegSqViAyL96EacZ9\nU0kLIiJ/34EfKCYQvwB8v4fdolZoZQeWD019DDwtAoGAYsl0TNwkEN2sw90s5XX5\n1kybd+emQ6PUBqm3cBtm8oObHEH061/rA00sqdlxuMpA+FIYbeEZyJAl0qbpui04\nB81KHzrjycho0MqnZKjclwU2qr+Qmdn5bT81IL9gmPH74JMcrZhe20bYpmIze1HO\nJFVJfTflxfZVUGiCwHwaw9I=\n-----END PRIVATE KEY-----\n",
        "client_email": "gsheets-loader@gifted-decker-309314.iam.gserviceaccount.com",
        "client_id": "118276584912755135813",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gsheets-loader%40gifted-decker-309314.iam.gserviceaccount.com"
    }
    gc = gspread.service_account_from_dict(data)
    sh = gc.open_by_key(doc)
    counter = 0
    done = True
    while True:
        print('iter' + str(counter))
        try:
            if len(list(filter(lambda w: w.title == title, sh.worksheets()))) == 0:
                sh.add_worksheet(title=title, rows="1", cols="1")

            if force:
                if len(list(filter(lambda w: w.title == title, sh.worksheets()))) > 0:
                    worksheet = sh.worksheet(title)
                    sh.del_worksheet(worksheet)

                worksheet = sh.add_worksheet(title=title, rows="1", cols="1")

            worksheet = sh.worksheet(title)
            set_with_dataframe(worksheet, data_frame, include_index=index)
            worksheet.format('1:1', {
                'textFormat': {'bold': True},
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE',
                'wrapStrategy': 'WRAP'
            })
        except requests.exceptions.ReadTimeout:
            print("reconnecting...")
            time.sleep(3)
        counter += 1
        print(counter)
        if (counter == 3) | (done == True):
            if (counter == 3):
                print('ReadTimeoutError')
            else:
                print('done')
            break

    if len(formats) > 0:
        for f in formats:
            worksheet.format(f['range'], f['value'])

def add_day(data_frame, attr):
    data_frame[attr] = pd.to_datetime(data_frame[attr])
    data_frame.loc[:, 'day'] = data_frame[attr].dt.normalize()
    return data_frame

def loader_iss_full_data(ticker):
    dfs = []
    for year in pd.date_range(pd.to_datetime('2000-01-01'), pd.to_datetime('2024-01-01'), freq=pd.DateOffset(years=1)).strftime('%Y-%m-%d').tolist():
        interval = 24
        till = (pd.to_datetime(year)+pd.DateOffset(years=1)).strftime('%Y-%m-%d')
        q = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.csv?from={year}&till={till}&interval={interval}'
        df = pd.read_csv(q,
                         sep=';',
                         header=1)
        df['ticker'] = ticker
        dfs.append(df)
    full = pd.concat(dfs, sort=False, ignore_index=True)
    full['begin'] = pd.to_datetime(full['begin'])
    full['end'] = pd.to_datetime(full['end'])
    return full


def stocks():
    listing = pd.read_csv('ListingSecurityList.csv', encoding='windows-1251')
    listing['TRADE_CODE'] = listing['TRADE_CODE'].str.lower()
    tickers = list(set(listing[listing['SUPERTYPE'].isin(['Акции']) &
                               (listing['TRADE_CODE'].notna())]['TRADE_CODE'].values) \
                   .difference(set(listing[(listing['SUPERTYPE'].isin(['Акции'])) &
                                           (listing['TRADE_CODE'].str.contains('-rm'))]['TRADE_CODE'].values))
                   )

    lists = []
    yahoo = []
    last_price = []
    for ticker in tickers:
        time.sleep(1)
        print(ticker)
        if requests.get('https://www.dohod.ru/ik/analytics/dividend/{}'.format(ticker)).status_code < 400:
            a = pd.read_html('https://www.dohod.ru/ik/analytics/dividend/{}'.format(ticker))
            a = a[1]
            a['ticker'] = ticker
            lists.append(a)
            print('downloading ', ticker)
            data = loader_iss_full_data(ticker)
            if data.empty == False:
                data['close'] = data['close'].astype(float)
                data['ticker'] = ticker
                data = data.reset_index()
                last_price.append(data.sort_values('end').groupby('ticker').tail(1)[['ticker', 'close']].rename(
                    columns={'close': 'Цена, посл'}))
                data['year_avg'] = data.groupby(data['begin'].dt.year)['close'].transform('median')
                data['year'] = data['begin'].dt.year
                data = data[['ticker', 'year', 'year_avg']].drop_duplicates()
                yahoo.append(data)

    dividends = pd.concat(lists, sort=False)
    dividends = dividends[dividends['Год'] != 'след 12m. (прогноз)']
    dividends['Год'] = dividends['Год'].astype(int)

    df_yahoo = pd.concat(yahoo, sort=False, ignore_index=True)
    df_last_price = pd.concat(last_price, sort=False, ignore_index=True)

    years = [i for i in range(2010, dt.datetime.now().year)]
    index = pd.MultiIndex.from_product([years, tickers], names=['year', 'ticker'])
    df_info = pd.DataFrame(index=index).reset_index().sort_values(['year', 'ticker'])

    df_info = df_info \
        .merge(dividends \
               .rename(columns={'Год': 'year'}),
               on=['year', 'ticker'],
               how='left') \
        .merge(df_yahoo,
               on=['year', 'ticker'],
               how='left')

    df_info.loc[df_info['Дивиденд (руб.)'] < 0.000001, 'Дивиденд (руб.)'] = np.nan
    df_info['div_doh'] = df_info['Дивиденд (руб.)'] / df_info['year_avg']

    df_info = df_info.merge(df_info \
                            .groupby('ticker') \
                            .agg({'Дивиденд (руб.)': 'mean'}) \
                            .reset_index() \
                            .rename(columns={'Дивиденд (руб.)': 'avg_div'}),
                            on='ticker',
                            how='left') \
        .merge(df_info.groupby('ticker') \
               .agg({'Дивиденд (руб.)': 'var'}) \
               .reset_index() \
               .rename(columns={'Дивиденд (руб.)': 'variance_div'}),
               on='ticker',
               how='left')

    df_info_dsi = df_info[(df_info['year'] >= dt.datetime.now().year - 7)
                          & (df_info['year'] < dt.datetime.now().year)] \
        .groupby(['ticker']) \
        .agg({'Дивиденд (руб.)': 'count', 'year': 'count'}) \
        .reset_index() \
        .sort_values('year') \
        .rename(columns={'Дивиденд (руб.)': 'count_div_pays'})
    df_info_dsi['DSI_7_years'] = df_info_dsi['count_div_pays'] / df_info_dsi['year']

    df_info = df_info.merge(df_info_dsi[['ticker', 'count_div_pays', 'DSI_7_years']], on='ticker', how='left') \
        .merge(df_info \
               .groupby(['ticker']) \
               .agg({'div_doh': 'mean'}) \
               .reset_index() \
               .rename(columns={'div_doh': 'avg_div_doh'}),
               on='ticker',
               how='left')[['ticker', 'avg_div',
                            'variance_div', 'avg_div_doh',
                            'count_div_pays', 'DSI_7_years']].drop_duplicates() \
        .merge(df_last_price,
               on='ticker',
               how='left') \
        .merge(listing[['TRADE_CODE', 'LIST_SECTION', 'EMITENT_FULL_NAME']],
               how='left',
               left_on='ticker',
               right_on='TRADE_CODE') \
        .drop('TRADE_CODE', axis=1)

    df_info['current_doh'] = df_info['avg_div'] / df_info['Цена, посл']
    df_info['delta'] = (df_info['avg_div_doh'] - df_info['current_doh']) * 100

    filtered = df_info[(df_info['current_doh'] > 0.06) &
            (df_info['avg_div_doh'] > 0.06) &
            (df_info['count_div_pays'].isin([7, 6, 5]))].sort_values(['delta'], ascending=[True])
    filtered['updated'] = pd.to_datetime(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return filtered


def bonds(corp=True):
    listing = pd.read_csv('ListingSecurityList.csv', encoding='windows-1251')
    bonds_df = pd.DataFrame(
        requests.get('https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json').json()['securities'][
            'data'],
        columns=
        requests.get('https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json').json()['securities'][
            'columns'])[['SHORTNAME',
                         'ISIN',
                         'MATDATE',
                         'OFFERDATE',
                         'NEXTCOUPON',
                         'COUPONVALUE',
                         'ACCRUEDINT',
                         'FACEVALUE',
                         'ISSUESIZE',
                         'LISTLEVEL',
                         'STATUS',
                         'COUPONPERIOD',
                         'CURRENCYID',
                         'SECNAME']] \
        .merge(pd.DataFrame(
        requests.get('https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json').json()['marketdata'][
            'data'],
        columns=
        requests.get('https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json').json()['marketdata'][
            'columns'])[['SECID',
                         'LAST',
                         'MARKETPRICETODAY',
                         'YIELD',
                         'CLOSEYIELD',
                         'DURATION',
                         'VALTODAY_RUR',
                         'VOLTODAY']] \
               .rename(columns={'SECID': 'ISIN'}),
               on='ISIN',
               how='left') \
        .merge(listing[(listing['SUPERTYPE'] == 'Облигации') &
                       (listing['ISIN'].notna())][['ISIN',
                                                   'INSTRUMENT_TYPE']],
               on='ISIN',
               how='left')
    for col in ['MATDATE', 'OFFERDATE', 'NEXTCOUPON']:
        bonds_df[col] = pd.to_datetime(bonds_df[col].replace('0000-00-00', np.nan, regex=True))
    bonds_df['ACCRUEDINT'] = round(bonds_df['ACCRUEDINT'], 2).fillna(0)
    bonds_df['YEARS_TO_CLOSE'] = round(
        (bonds_df['MATDATE'] - pd.to_datetime(dt.datetime.now().date())) / np.timedelta64(1, 'Y'), 1).fillna(0)
    bonds_df['DAYS_TO_COUPON'] = round(
        (bonds_df['NEXTCOUPON'] - pd.to_datetime(dt.datetime.now().date())) / np.timedelta64(1, 'D'), 0).fillna(
        0).astype(int)
    bonds_df['CNT_COUPON_IN_YEAR'] = round(365 / bonds_df['COUPONPERIOD'], 0).replace(np.inf, np.nan).fillna(0).astype(
        int)
    bonds_df['DURATION_YEARS'] = round(bonds_df['DURATION'] / 365, 2)
    bonds_df['LAST'] = round(bonds_df['LAST'].fillna(bonds_df['MARKETPRICETODAY']), 2)
    bonds_df['CALCULATED_YIELD'] = round(((1000 - bonds_df['LAST'] * 10 + (
                bonds_df['YEARS_TO_CLOSE'] * bonds_df['CNT_COUPON_IN_YEAR'] * bonds_df['COUPONVALUE'] - bonds_df[
            'ACCRUEDINT'])) / (bonds_df['LAST'] * 10) * (1 / bonds_df['YEARS_TO_CLOSE'])) * 100, 2)
    bonds_df = bonds_df[(bonds_df['MATDATE'].notna()) &
                        (bonds_df['LAST'].notna()) &
                        (bonds_df['DURATION'].notna()) &
                        (bonds_df['INSTRUMENT_TYPE'].notna()) &
                        (bonds_df['CURRENCYID'] == 'SUR') &
                        (bonds_df['INSTRUMENT_TYPE'].isin(['Облигация биржевая',
                                                           'Облигация корпоративная',
                                                           'Облигация субфедеральная',
                                                           'Облигация федерального займа']))] \
        .sort_values(['ISIN', 'YIELD']) \
        .groupby('ISIN') \
        .tail(1)[
        ['SHORTNAME', 'SECNAME', 'ISIN', 'MATDATE', 'YEARS_TO_CLOSE', 'OFFERDATE', 'NEXTCOUPON', 'LAST', 'YIELD',
         'CALCULATED_YIELD', 'DURATION_YEARS', 'VALTODAY_RUR', 'VOLTODAY',
         'COUPONVALUE', 'ACCRUEDINT', 'ISSUESIZE', 'DAYS_TO_COUPON', 'LISTLEVEL',
         'STATUS', 'CNT_COUPON_IN_YEAR', 'INSTRUMENT_TYPE']].rename(columns={'SHORTNAME': 'Имя',
                                                                             'SECNAME': 'Имя полное',
                                                                             'MATDATE': 'Погашение',
                                                                             'OFFERDATE': 'Оферта',
                                                                             'NEXTCOUPON': 'Дата купона',
                                                                             'COUPONVALUE': 'Купон, руб',
                                                                             'ACCRUEDINT': 'НКД, руб',
                                                                             'ISSUESIZE': 'Объем выпуска, штук',
                                                                             'LISTLEVEL': 'Уровень листинга',
                                                                             'STATUS': 'Статус',
                                                                             'LAST': 'Цена',
                                                                             'YIELD': 'Доходность по последней сделке,%',
                                                                             'INSTRUMENT_TYPE': 'Тип облигации',
                                                                             'YEARS_TO_CLOSE': 'Лет до погаш.',
                                                                             'DAYS_TO_COUPON': 'Дней до купона',
                                                                             'CNT_COUPON_IN_YEAR': 'Частота, раз в год',
                                                                             'DURATION_YEARS': 'Дюрация, лет',
                                                                             'CALCULATED_YIELD': 'Доходность к погашению,%',
                                                                             'VOLTODAY': 'Объем в бумагах, шт',
                                                                             'VALTODAY_RUR': 'Объем в валюте'})

    if corp:
        filtered_bonds_df = bonds_df[(bonds_df['Лет до погаш.'] <= 3)
                                          & (bonds_df['Доходность к погашению,%'] <= 20)
                                          & (bonds_df['Доходность к погашению,%'] > 7)
                                          & (bonds_df['Дюрация, лет'] <= 3)
                                          & (bonds_df['Уровень листинга'].isin([1, 2]))
                                          & (bonds_df['Объем в бумагах, шт'] > 0)
                                          & (bonds_df['Тип облигации'].isin(
            ['Облигация биржевая', 'Облигация корпоративная']))] \
            .sort_values(['Доходность к погашению,%', 'Доходность по последней сделке,%', 'Цена', 'Объем в валюте'],
                         ascending=[False, False, True, False]) \
            .reset_index(drop=True)
    else:
        filtered_bonds_df = bonds_df[(bonds_df['Лет до погаш.'] <= 3)
                                         & (bonds_df['Доходность к погашению,%'] <= 20)
                                         & (bonds_df['Доходность к погашению,%'] > 7)
                                         & (bonds_df['Дюрация, лет'] <= 3)
                                         & (bonds_df['Уровень листинга'].isin([1, 2]))
                                         & (bonds_df['Объем в бумагах, шт'] > 0)
                                         & (bonds_df['Тип облигации'].isin(
            ['Облигация субфедеральная', 'Облигация федерального займа']))] \
            .sort_values(['Доходность к погашению,%', 'Доходность по последней сделке,%', 'Цена', 'Объем в валюте'],
                         ascending=[False, False, True, False]) \
            .reset_index(drop=True)
    return filtered_bonds_df

@bot.message_handler(commands=["start"])
def start(message):
    bot.send_message(message.chat.id, 'Bot is started.\n "Div" - dividend stocks\n "Bnd" - actual corporate bonds')

@bot.message_handler(content_types=['text'])
def get_text_message(message):
    if message.text == 'Div':
        bot.send_message(message.from_user.id, 'Dividend stocks counting started')
        final = stocks()
        load_df_to_gsheets(final,
                   "1p2_FlOkFGkBzDpOYv4Hi2gChFvnbNAZQGHmnc_kDkkk",
                   'Дивидендная стратегия')
        bot.send_message(message.from_user.id, 'Таблица обновлена : \n https://docs.google.com/spreadsheets/d/1p2_FlOkFGkBzDpOYv4Hi2gChFvnbNAZQGHmnc_kDkkk/')
    elif message.text == 'Bnd':
        bot.send_message(message.from_user.id, 'Corporate bonds counting started')
        final_b = bonds()
        load_df_to_gsheets(final_b,
                           "1p2_FlOkFGkBzDpOYv4Hi2gChFvnbNAZQGHmnc_kDkkk",
                           'Корпоративные облигации')
        bot.send_message(message.from_user.id,
                         'Таблица обновлена : \n https://docs.google.com/spreadsheets/d/1p2_FlOkFGkBzDpOYv4Hi2gChFvnbNAZQGHmnc_kDkkk/')
    elif message.text == 'Fed':
        bot.send_message(message.from_user.id, 'Subfed/Fed bonds counting started')
        final_bf = bonds(False)
        load_df_to_gsheets(final_bf,
                           "1p2_FlOkFGkBzDpOYv4Hi2gChFvnbNAZQGHmnc_kDkkk",
                           'Государственные облигации')
        bot.send_message(message.from_user.id,
                         'Таблица обновлена : \n https://docs.google.com/spreadsheets/d/1p2_FlOkFGkBzDpOYv4Hi2gChFvnbNAZQGHmnc_kDkkk/')
    else:
        bot.send_message(message.from_user.id, 'unknown command')

keep_alive()

if __name__ == '__main__':
    print('Start bot')
    bot.polling(none_stop=True, interval=0)

