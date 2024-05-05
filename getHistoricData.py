from batch import *

# postgres_engine = invoke_db_engine()


def fetch_crypto_details(id):
    try:
        print("Fetching data from api")
        url = f"https://api.coincap.io/v2/assets/{id}/history?interval=d1"
        headers = {
            "Authorization": "Bearer 66bc99f3-6892-4b63-a1a8-d7caa946baf5"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            if response.text is not None and len(response.text) > 0:
                print("Data fetched successfully from API")
                return (json.loads(response.text))
            raise Exception("No data available from source")
        else:
            raise Exception("Unable to communicate with api")
    except Exception as e:
        print(f"Exception occurred while fetching the data {e}")


cryptocurrencies = [
    "bitcoin", "ethereum", "tether", "binance-coin", "solana", "usd-coin", "xrp", "cardano", "dogecoin",
    "avalanche", "shiba-inu", "polkadot", "tron", "chainlink", "polygon", "wrapped-bitcoin", "near-protocol",
    "bitcoin-cash", "uniswap", "litecoin", "internet-computer", "unus-sed-leo", "multi-collateral-dai",
    "render-token", "filecoin", "ethereum-classic", "the-graph", "stacks", "okb", "stellar", "crypto-com-coin",
    "theta", "injective-protocol", "vechain", "cosmos", "thorchain", "maker", "monero", "arweave", "fetch",
    "lido-dao", "fantom", "algorand", "flow", "aave", "gala", "hedera-hashgraph", "bitcoin-sv", "conflux-network",
    "elrond-egld", "quant", "singularitynet", "axie-infinity", "the-sandbox", "kucoin-token", "mina", "akash-network",
    "tezos", "chiliz", "decentraland", "helium", "trueusd", "eos", "0x", "neo", "ecash", "wemix", "kava",
    "synthetix-network-token", "klaytn", "aioz-network", "gnosis-gno", "wootrade", "curve-dao-token",
    "nervos-network", "nexo", "enjin-coin", "bitcoin-gold", "ribbon-finance", "livepeer", "iotex", "gatetoken",
    "ocean-protocol", "1inch", "raydium", "celo", "ftx-token", "pendle", "dydx", "superfarm", "compound",
    "frax-share", "holo", "loopring", "zilliqa", "trust-wallet-token", "rocket-pool"
]

for cryptoid in cryptocurrencies:
    data = fetch_crypto_details(cryptoid)
    filtered_data = pd.DataFrame([])
    for i in data.get("data"):
        print(i)
        new_df = pd.DataFrame([{"id": cryptoid, 'price': i.get("priceUsd"),
                                    "fetched_at":  datetime.fromisoformat(i.get("date")[:-1]).date() }])
        filtered_data = pd.concat([filtered_data, new_df], ignore_index=True)
    print(filtered_data)


    print(f"inserting into DB for {i}")
    filtered_data.to_sql('crypto_historic_data', con=postgres_engine, if_exists='append', index=False)
