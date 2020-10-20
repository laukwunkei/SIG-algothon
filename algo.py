'''
SIG algothon
Run this in Quantopian platform
'''
#Pipeline modules
import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline

#Filters and factors 
from quantopian.pipeline.filters import StaticAssets
from quantopian.pipeline.factors import CustomFactor, Returns

#Datasets
from quantopian.pipeline.data.builtin import USEquityPricing

#Optimize
import quantopian.optimize as opt
 
# Analytics packages
import numpy as np
import pandas as pd

def initialize(context): 
    set_benchmark(symbol('QQQ')) #Comparable benchmarks
    set_commission(commission.PerShare(cost=0.00, min_trade_cost=0.00)) #assume 0 transaction costs
    context.SUSPEND_DAYS = 14 # Min. days to be out of the market
    context.DBB = symbol('DBB') # metals signals
    context.XLI = symbol('XLI') # industrials signals 
    context.BIL = symbol('SHY') # debt market signals
    context.UUP = symbol('UUP') # currency signals
    
    context.SIGNAL_UNIVERSE = symbols('XLI', 'DBB', 'SHY', 'UUP') 
    
    # Asset allocation when we are in and out of the market
    context.TRADE_WEIGHTS_OUT = {symbol('IEF'): 0.5, symbol('TLT'):0.5 # Long treasury bond ETF when out of the market
                                }
    context.TRADE_WEIGHTS_IN = {symbol('SPY'): 1.0
                                }            
    # Schedule functions to update portfolio 
    schedule_function(  
        # daily rebalance if OUT of the market
        rebalance_when_out_of_the_market,  
        date_rules.every_day(),
        time_rules.market_open()
    ) 
    schedule_function(  
        # weekly rebalance if IN the market
        rebalance_when_in_the_market,  
        date_rules.week_start(days_offset=4),
        time_rules.market_open()
    )
    schedule_function(  
        # record signals every day
        record_signals,  
        date_rules.every_day(),
        time_rules.market_open()
    )
    
    algo.attach_pipeline(make_pipeline(context), 'pipeline')  

class Days_Since_True(CustomFactor): 
    #Find the number of days from triggering last signal
    window_length = 90
    def compute(self, today, assets, out, input_factor): 
        # Flip the factor first to last so argmax finds the last occurance
        # Set the earliest day to True so it always finds at least 1 True 
        input_factor[0] = True
        out[:] = np.argmax(np.flipud(input_factor), axis=0)        

def make_pipeline(context):
    # Set universe to only the assets we use for signals
    mask = StaticAssets(context.SIGNAL_UNIVERSE)
    
    # Get the returns for basic metals, industrials and bonds
    # Get the cost of debt which is the inverse the bond returns
    returns = Returns(window_length=60, mask=mask)
    
    basic_metals_down = returns[context.DBB] < -.07
    industrial_sector_down = returns[context.XLI] < -.07
    short_term_bond_down = returns[context.BIL] < -.01
    cost_of_debt_up = short_term_bond_down
    dollar_up = returns[context.UUP] > .07
    
    # Bear signal if any one of the above is triggered
    bear_signal = (basic_metals_down |
                   industrial_sector_down |
                   cost_of_debt_up |
                   dollar_up 
                   )
    days_since_last_bear_signal = Days_Since_True([bear_signal], mask=mask)

    # Go out of the market if bear in recent days
    go_out_of_the_market = days_since_last_bear_signal < context.SUSPEND_DAYS 
                            
    # All we really need is go_out_of_the_market. 
    # Others are for recording and debug if needed.
    pipe = Pipeline(columns={ 
            'go_out_of_the_market': go_out_of_the_market,
            'bear_signal': bear_signal,
            'days_since_last_bear_signal': days_since_last_bear_signal,
            'basic_metals_down': Days_Since_True([basic_metals_down], mask=mask) < context.SUSPEND_DAYS,
            'industrial_sector_down': Days_Since_True([industrial_sector_down], mask=mask) < context.SUSPEND_DAYS,
            'cost_of_debt_up': Days_Since_True([cost_of_debt_up], mask=mask) < context.SUSPEND_DAYS,
            'dollar_up': Days_Since_True([dollar_up], mask=mask) < context.SUSPEND_DAYS,
            },
            screen=mask
    )
    return pipe

def rebalance_when_out_of_the_market(context, data):
    # Check whether triggered out of market sign
    df = algo.pipeline_output('pipeline') 
    go_out_of_the_market = df.go_out_of_the_market[0]
 
    if go_out_of_the_market:
        # Long bonds
        order_optimal_portfolio(  
            objective = opt.TargetWeights(context.TRADE_WEIGHTS_OUT), 
            constraints = []  
        )
        
def rebalance_when_in_the_market(context, data):
    # Get signal. All rows are the same. Just choose the first one.
    df = algo.pipeline_output('pipeline') 
    go_out_of_the_market = df.go_out_of_the_market[0]
 
    if not go_out_of_the_market:
        # Long SPY
        order_optimal_portfolio(  
            objective = opt.TargetWeights(context.TRADE_WEIGHTS_IN), 
            constraints = []  
        )
        
def record_signals(context, data):
    df = algo.pipeline_output('pipeline') 

    in_the_market = not df.go_out_of_the_market[0]
    dollar = df.dollar_up[0]
    debt = df.cost_of_debt_up[0]
    metals = df.basic_metals_down[0]
    industrials = df.industrial_sector_down[0]
    # Testing
    record(in_the_market=in_the_market, dollar=dollar, debt=debt, metals=metals, industrials=industrials)