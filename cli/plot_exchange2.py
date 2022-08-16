import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd
import sys

# Auto-detect terminal width.
pd.options.display.width = None
pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 200

DATE = pd.to_datetime('2020-01-01')
DAYS = 10

MKT_OPEN = pd.to_timedelta('09:00:00')
MKT_CLOSE =pd.to_timedelta('16:00:00')

BETWEEN_START = DATE + MKT_OPEN
BETWEEN_END = DATE + MKT_CLOSE+ pd.to_timedelta(DAYS, unit='D')



# Linewidth for plots.
LW = 1

# Fidelity of plot
FIDELITY = 100

# Main program starts here.

if len(sys.argv) < 2:
  print ("Usage: python plot_exchange.py <Simulator Fundamental file>")
  sys.exit()

sim_file = sys.argv[1]

df_sim = pd.read_pickle(sim_file, compression='bz2')

#print(df_sim)

df_bid = df_sim.loc[df_sim['EventType'] == 'BEST_BID']
df_bid = df_bid.assign( BID_PRICE = lambda x: x['Event'].str.split(',').str[1].astype('float64'))

df_ask = df_sim.loc[df_sim['EventType'] == 'BEST_ASK']
df_ask = df_ask.assign( ASK_PRICE = lambda x: x['Event'].str.split(',').str[1].astype('float64'))

df_trade = df_sim.loc[df_sim['EventType'] == 'LAST_TRADE']
df_trade = df_trade.assign( TRADE_PRICE = lambda x: x['Event'].str.replace("$", " ").str.split(',').str[1].astype('float64'))
df_trade = df_trade.assign( TRADE_SIZE = lambda x: x['Event'].str.replace("$", " ").str.split(',').str[0].astype('float64'))

#print(df_trade)

plt.rcParams.update({'font.size': 12})

fig, axes = plt.subplots(figsize=(12,9), nrows=1, ncols=DAYS, sharey=True)
fig.subplots_adjust(wspace=0.05)

# filter dfs to plot fewer values
df_bid = df_bid.iloc[::FIDELITY]
df_ask = df_ask.iloc[::FIDELITY]
df_trade = df_trade.iloc[::FIDELITY]

# filter df to plot date range 
df_bid = df_bid.loc[df_bid.index >= BETWEEN_START]
df_bid = df_bid.loc[df_bid.index <= BETWEEN_END]

df_ask = df_ask.loc[df_ask.index >= BETWEEN_START]
df_ask = df_ask.loc[df_ask.index <= BETWEEN_END]

# turn prices into dollars
df_bid = df_bid.assign( BID_PRICE = lambda x: x['BID_PRICE'] / 100)
df_ask = df_ask.assign( ASK_PRICE = lambda x: x['ASK_PRICE'] / 100)
df_trade = df_trade.assign( TRADE_PRICE = lambda x: x['TRADE_PRICE'] / 100)

# filter df to shorten market close
dfs = []
df_bid_trimmed = pd.DataFrame()
for day in range(DAYS):
  df_bid_temp = df_bid.loc[df_bid.index <= (DATE + MKT_CLOSE + pd.to_timedelta(day, unit='D'))]
  df_bid_temp = df_bid_temp.loc[df_bid_temp.index >= (DATE + MKT_OPEN + pd.to_timedelta(day, unit='D'))]
  df_bid_trimmed = df_bid_trimmed.append(df_bid_temp)
  dfs.append(df_bid_temp)

df_bid = df_bid_trimmed

for i, df in enumerate(dfs):
    df['BID_PRICE'].plot(color='C1', grid=True, linewidth=LW, alpha=0.9, ax=axes[i])

    axes[i].spines.right.set_visible(False)
    axes[i].spines.left.set_visible(False)

    datemax = DATE + MKT_CLOSE + pd.to_timedelta(i, unit='D')
    datemin = DATE + MKT_OPEN + pd.to_timedelta(i, unit='D')
    print(datemin, datemax)
    axes[i].set_xlim(datemin, datemax)
    axes[i].set_xticks(pd.date_range(datemin, datemax, freq='1D'))
    # hide x label 
    axes[i].set_xlabel('')

axes[0].tick_params(labelright='off')
axes[0].yaxis.tick_left()
axes[1].yaxis.tick_right()
axes[0].legend(['BID_PRICE', 'ASK_PRICE', "TRADE_PRICE"])
axes[0].set_title("Exchange Agent")

axes[0].set_ylabel('Price ($)')
axes[3].set_xlabel('Time')

#plt.savefig('value_noise_MM_2''.png')


plt.show()



#############################

