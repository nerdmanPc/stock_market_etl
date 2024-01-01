create table if not exists price_data (
    tick text,
    timestamp numeric,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    adjusted_close numeric,
    volume numeric,
    dividend_amount numeric,
    primary key (tick, timestamp),
    foreign key (tick) references company_data (tick)
        on delete cascade 
        on update no action
);

create table if not exists earnings_data (
    tick text,
    timestamp numeric,--fiscal_Date_Ending,
    reported_Date numeric,
    reported_EPS numeric,
    estimated_EPS numeric,
    surprise numeric,
    surprise_Percentage numeric,
    primary key (tick, timestamp),--fiscal_Date_Ending),
    foreign key (tick) references company_data (tick)
        on delete cascade 
        on update no action
);

create table if not exists cashflow_data (
    tick text,
    timestamp numeric,--fiscal_Date_Ending,
    reported_Currency text,
    operating_Cashflow numeric,
    payments_For_Operating_Activities numeric,
    proceeds_From_Operating_Activities numeric,
    change_In_Operating_Liabilities numeric,
    change_In_Operating_Assets numeric,
    depreciation_Depletion_And_Amortization numeric,
    capital_Expenditures numeric,
    change_In_Receivables numeric,
    change_In_Inventory numeric,
    profit_Loss numeric,
    cashflow_From_Investment numeric,
    cashflow_From_Financing numeric,
    proceeds_From_Repayments_Of_Short_Term_Debt numeric,
    payments_For_Repurchase_Of_Common_Stock numeric,
    payments_For_Repurchase_Of_Equity numeric,
    payments_For_Repurchase_Of_Preferred_Stock numeric,
    dividend_Payout numeric,
    dividend_Payout_Common_Stock numeric,
    dividend_Payout_Preferred_Stock numeric,
    proceeds_From_Issuance_Of_Common_Stock numeric,
    proceeds_From_Issuance_Of_Long_Term_Debt_And_Capital_Securities_Net numeric,
    proceeds_From_Issuance_Of_Preferred_Stock numeric,
    proceeds_From_Repurchase_Of_Equity numeric,
    proceeds_From_Sale_Of_Treasury_Stock numeric,
    change_In_Cash_And_Cash_Equivalents numeric,
    change_In_Exchange_Rate numeric,
    net_Income numeric,
    primary key (tick, timestamp),--fiscal_Date_Ending),
    foreign key (tick) references company_data (tick)
        on delete cascade 
        on update no action
);

create table if not exists income_statement (
    tick text,
    timestamp numeric,--fiscal_Date_Ending,
    reported_Currency text,
    gross_Profit numeric,
    total_Revenue numeric,
    cost_Of_Revenue numeric,
    costof_Goods_And_Services_Sold numeric,
    operating_Income numeric,
    selling_General_And_Administrative numeric,
    research_And_Development numeric,
    operating_Expenses numeric,
    investment_Income_Net numeric,
    net_Interest_Income numeric,
    interest_Income numeric,
    interest_Expense numeric,
    non_Interest_Income numeric,
    other_Non_Operating_Income numeric,
    depreciation numeric,
    depreciation_And_Amortization numeric,
    income_Before_Tax numeric,
    income_Tax_Expense numeric,
    interest_And_Debt_Expense numeric,
    net_Income_From_Continuing_Operations numeric,
    comprehensive_Income_Net_Of_Tax numeric,
    ebit numeric,
    ebitda numeric,
    net_Income numeric,
    primary key (tick, timestamp),--fiscal_Date_Ending),
    foreign key (tick) references company_data (tick)
        on delete cascade 
        on update no action
);

create table if not exists balance_sheet(
    tick text,
    timestamp numeric,--fiscal_Date_Ending,
    reported_Currency text,
    total_Assets numeric,
    total_Current_Assets numeric,
    cash_And_Cash_Equivalents_At_Carrying_Value numeric,
    cash_And_Short_Term_Investments numeric,
    inventory numeric,
    current_Net_Receivables numeric,
    total_Non_Current_Assets numeric,
    property_Plant_Equipment numeric,
    accumulated_Depreciation_Amortization_PPE numeric,
    intangible_Assets numeric,
    intangible_Assets_Excluding_Goodwill numeric,
    goodwill numeric,
    investments numeric,
    long_Term_Investments numeric,
    short_Term_Investments numeric,
    other_Current_Assets numeric,
    other_Non_Current_Assets numeric,
    total_Liabilities numeric,
    total_Current_Liabilities numeric,
    current_Accounts_Payable numeric,
    deferred_Revenue numeric,
    current_Debt numeric,
    short_Term_Debt numeric,
    total_Non_Current_Liabilities numeric,
    capital_Lease_Obligations numeric,
    long_Term_Debt numeric,
    current_Long_Term_Debt numeric,
    long_Term_Debt_Noncurrent numeric,
    short_Long_Term_Debt_Total numeric,
    other_Current_Liabilities numeric,
    other_Non_Current_Liabilities numeric,
    total_Shareholder_Equity numeric,
    treasury_Stock numeric,
    retained_Earnings numeric,
    common_Stock numeric,
    common_Stock_Shares_Outstanding numeric,
    primary key (tick, timestamp),--fiscal_Date_Ending),
    foreign key (tick) references company_data (tick)
        on delete cascade 
        on update no action
);

create table if not exists company_data (
    tick text primary key,
    Asset_Type text,
    Name text,
    Description text,
    CIK numeric,
    Exchange text,
    Currency text,
    Country text,
    Sector text,
    Industry text,
    Address text,
    Fiscal_Year_End text,
    Latest_Quarter text,
    Market_Capitalization numeric,
    EBITDA numeric,
    PE_Ratio numeric,
    PEG_Ratio numeric,
    Book_Value numeric,
    Dividend_Per_Share numeric,
    Dividend_Yield numeric,
    EPS numeric,
    Revenue_Per_Share_TTM numeric,
    Profit_Margin numeric,
    Operating_Margin_TTM numeric,
    Return_On_Assets_TTM numeric,
    Return_On_Equity_TTM numeric,
    Revenue_TTM numeric,
    Gross_Profit_TTM numeric,
    Diluted_EPS_TTM numeric,
    Quarterly_Earnings_Growth_YOY numeric,
    Quarterly_Revenue_Growth_YOY numeric,
    Analyst_Target_Price numeric,
    Trailing_PE numeric,
    Forward_PE numeric,
    Price_To_Sales_Ratio_TTM numeric,
    Price_To_Book_Ratio numeric,
    EV_To_Revenue numeric,
    EV_To_EBITDA numeric,
    Beta numeric,
    High_52_Week numeric,
    Low_52_Week numeric,
    Moving_Average_50_Day numeric,
    Moving_Average_200_Day numeric,
    Shares_Outstanding numeric,
    Dividend_Date numeric,
    Ex_Dividend_Date numeric
);