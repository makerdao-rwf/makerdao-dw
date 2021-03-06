# Makerdao Datawarehouse

ETL for MakerDAO RWF Core Unit Data Warehouse.


## Installation

Require Python 3|.

You will need PostgreSQL dev libs. If under Ubuntu 20.04:

    apt install libpq-dev

You will have to install some package 

    pip install pyhocon pandas psycopg2 eth_event web3 sqlachemy

You will also need a Infura key and access to a PostgreSQL database.

## Configuration

You need to create a prod.conf or dev.conf (doesn't matter) that is based on template.conf. Current

    infura_key = 'xxxxxxxxxxxxx'

    db {
      host = "<host>"
      user = "<user>"
      password = "<password>"
      database = "<db>"
    }

Configuration is managed in [HOCON format](https://github.com/chimpler/pyhocon) which is a superset of JSON.

## Usage

### Updating the ethereum.blocks table

The following command will update the ethereum.blocks table (and create the schema/table if needed).

    python eth-blocks.py ethereum.transactions

The table will have the following format:

block_number|block_hash                                                      |miner                                   |nonce           |gas_limit|gas_used|difficulty |extra_data                                                      |time               |size
------------|----------------------------------------------------------------|----------------------------------------|----------------|---------|--------|-----------|----------------------------------------------------------------|-------------------|----
0 | D4E5... | 0000... | 0000... |     5000 |       0| 17179869184 | 11BB... | 1970-01-01 00:00:00 | 540
1|88E9...|05A5...|539B...|     5000|       0|17171480576|4765...|2015-07-30 15:26:28| 537


### Updating the contracts tables


The following command will update the *schema*.*contract_** tables (and create the schema/tables if needed).

    python eth-contract.py makermcd.psm

Two sets of tables are generated:
- *contract_call_function* where *function* is a function (excluding view only functions)
- *contract_evt_event* where *function* is an event (excluding anonymous events)

For instance, here is the structure of the makermcd.psm_evt_buygem table:

block_number|block_hash                                                      |address                                 |log_index|transaction_index|transaction_hash                                                |owner                                   |value         |fee                    |
------------|----------------------------------------------------------------|----------------------------------------|---------|-----------------|----------------------------------------------------------------|----------------------------------------|--------------|-----------------------|
11550321|A06F...|89B7...|      128|               77|20A1...|3CB4...|       1000000|       8539166666666666|
11584138|9347...|89B7...|      231|              167|552B...|5617...|   10000000000|   18524851190476190000|

### Adding a new contract

To add a new contract you need to provide configuration for the contract and its ABI.

Contract ABI are stored in conf/*schema*/*contract*.abi. Schema are a way to group contracts related to a single project.

Per schema/contracts group, a configuration file should be added under /conf/ and referenced from /config.conf.

```javascript
centrifuge {
  shelf {
    addresses : [
        "0x7d057A056939bb96D682336683C10EC89b78D7CE" // New Silver 2
        ,"0xA0B0d8394ADC79f5d1563a892abFc6186E519644" // Consol Freight 4
      ]
    creationBlock : 11063052
    blocksStep : 10000 // optional to overload the default. x blocks should return less than 10k logs for the contract.
  }
}
```
Once added, you will need to add your new .config file to the contract list in "config.conf".

### Changing ABI / Correcting a bug in a contract

The program always restart from the last block so if there was a problem, it's usually easy to just delete all the table related to the contract and run the program again.

## TODO

### Bugs

- [ ] Function param decoding crashing with cenrtifuge.shelf contract.

### Features

- [ ] Create tables only at the first row insertion (to avoid creating empty tables)
- [ ] Bulk load rows by batching them
- [ ] Updating a ethereum.transactions table when parsing blocks (full_transactions = True)
- [ ] Supporting Snowflake and PostgreSQL (connection string and byte array formatting)