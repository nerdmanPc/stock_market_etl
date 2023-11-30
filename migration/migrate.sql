create table if not exists price_data (
    tick,
    timestamp,
    open,
    high,
    low,
    close,
    adjusted_close,
    volume,
    dividend_amount,
    split_coefficient,
    primary key (tick, timestamp),
    foreign key (tick) references (company_data)
        on delete cascade 
        on update no action
);
create table if not exists earnings_data (
    tick,
    timestamp,
    reportedDate,
    reportedEPS,
    estimatedEPS,
    surprise,
    surprisePercentage,
    primary key (tick, timestamp),
    foreign key (tick) references (company_data)
        on delete cascade 
        on update no action
);
create table if not exists companies_data (
    tick primary key
);