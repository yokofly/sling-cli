timeplusd: 2.4.6 (other version shall work as well)


# work with timeplusd cli
## prepare data
```sql
CREATE STREAM trade_data
(
    trade_id uint64,
    tradedate int64,
    source string,
    symbol string,
    price float64,
    quantity int32,
    trade_type string,
    trader_id string
)
PARTITION BY (tradedate, source)
ORDER BY (tradedate, symbol);


insert into trade_data(trade_id, tradedate, source, symbol, price, quantity, trade_type, trader_id) values(1002, format_DateTime(now(), '%Y%m%d') ,'SNAP','GOOGL',140.25,50,'Sell','TRADER002')


```

## execute the bash script to clean the partitions(today's data)

```bash
./clean_partitions.sh
```


## ideal output

```sh
./clean_partitions.sh 
today :20241008
2024-10-08 07:14:17 - Processing table: trade_data
Retrieved partitions: (20241008,'SNAP')
(20241008,'NYSE')
Dropping partition: (20241008,'SNAP') from table: trade_data
Successfully dropped partition: (20241008,'SNAP') from table: trade_data
Dropping partition: (20241008,'NYSE') from table: trade_data
Successfully dropped partition: (20241008,'NYSE') from table: trade_data
2024-10-08 07:14:18 - Processing table: another_table
Retrieved partitions: 
No partitions to drop for table: another_table on date: 20241008
Partition cleanup completed on Tue 08 Oct 2024 07:14:18 AM PDT
```


## Usage

1. **Ensure Executable Permissions:**
   Make sure both `cleanup_partitions.sh` and `config.sh` have executable permissions.
   ```bash
   chmod +x cleanup_partitions.sh config.sh
   ```

2. **Run the Script:**
   Execute the script manually or set it up as a daily cron job.
   ```bash
   ./cleanup_partitions.sh
   ```

## Setting Up a Cron Job (Optional)

To automate the partition cleanup daily, you can set up a cron job. Here's how you can do it:

1. **Edit the Crontab:**
   ```bash
   crontab -e
   ```

2. **Add the Following Line to Schedule the Script to Run Daily at 2 AM:**
   ```bash
   0 2 * * * /path/to/cleanup_partitions.sh >> /var/log/cleanup_partitions.log 2>&1
   ```
   - Replace `/path/to/cleanup_partitions.sh` with the actual path to your script.
   - Logs will be appended to `/var/log/cleanup_partitions.log`. Ensure the script has the necessary permissions to write to this file or choose an appropriate log location.





# work with sling

```bash
sling clean-partitions -c config.yaml
```

## data preparation

```sql
insert into trade_data(trade_id, tradedate, source, symbol, price, quantity, trade_type, trader_id) values(1002, format_DateTime(now(), '%Y%m%d') ,'SNAP','GOOGL',140.25,50,'Sell','TRADER002')

insert into trade_data(trade_id, tradedate, source, symbol, price, quantity, trade_type, trader_id) values(1002, format_DateTime(now(), '%Y%m%d') ,'COCO','GOOGL',140.25,50,'Sell','TRADER002')

```

## output
```bash
(base) ➜  sling-cli git:(feature/clean-daily-partition) ✗ ./sling clean-partitions -c  config.yaml
8:18AM INF Dropped partition (20241008,'SNAP') from table trade_data
8:18AM INF Dropped partition (20241008,'COCO') from table trade_data
```